#!/usr/bin/env python3
"""Run semantic parity v2 checks for Python and Rust Postgres MCP servers.

The harness executes the shared fixture corpus against both implementations via
Rust mcp-probe stdio scripted runs, normalizes responses according to
fixtures/parity_v2/normalization_rules.json, validates per-implementation
expectations from tool_cases.json, and diffs normalized canonical models.
"""

from __future__ import annotations

import argparse
import ast
import copy
import json
import os
import shlex
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote, unquote, urlparse


DEFAULT_LOCAL_DB_URI = "postgresql://nbn_dev_user:nbn_dev_user@127.0.0.1:54322/nbn_dev"


@dataclass
class NormalizedResult:
    kind: str
    payload: Any | None = None
    error_message: str | None = None
    error_class: str | None = None
    raw_step_status: str | None = None
    raw_step_detail: str | None = None


def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def redact_database_uri(uri: str) -> str:
    parsed = urlparse(uri)
    if not parsed.password:
        return uri
    user = parsed.username or ""
    user_info = quote(user, safe="") if user else ""
    if user_info:
        user_info = f"{user_info}:***"
    else:
        user_info = "***"
    host = parsed.hostname or ""
    port = f":{parsed.port}" if parsed.port else ""
    netloc = f"{user_info}@{host}{port}"
    path = parsed.path or ""
    query = f"?{parsed.query}" if parsed.query else ""
    fragment = f"#{parsed.fragment}" if parsed.fragment else ""
    return f"{parsed.scheme}://{netloc}{path}{query}{fragment}"


def build_database_uri_from_pg_env() -> str | None:
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    host = os.getenv("POSTGRES_HOST")
    port = os.getenv("POSTGRES_PORT")
    database = os.getenv("POSTGRES_DB") or os.getenv("POSTGRESS_DB")

    if not (user and host and port and database):
        return None

    user_enc = quote(user, safe="")
    password_enc = quote(password or "", safe="")
    return f"postgresql://{user_enc}:{password_enc}@{host}:{port}/{database}"


def normalize_database_uri(uri: str) -> str:
    # Accept SQLAlchemy-style DSNs by stripping the driver suffix for libs that
    # expect a plain postgres conninfo string.
    if uri.startswith("postgresql+psycopg://"):
        return "postgresql://" + uri[len("postgresql+psycopg://") :]
    return uri


def resolve_database_uri(cli_uri: str | None) -> str:
    if cli_uri:
        return normalize_database_uri(cli_uri)

    env_uri = os.getenv("DATABASE_URI")
    if env_uri:
        return normalize_database_uri(env_uri)

    env_url = os.getenv("DATABASE_URL")
    if env_url:
        return normalize_database_uri(env_url)

    pg_uri = build_database_uri_from_pg_env()
    if pg_uri:
        return normalize_database_uri(pg_uri)

    return DEFAULT_LOCAL_DB_URI


def parse_command(command: str) -> tuple[str, list[str]]:
    parts = shlex.split(command)
    if not parts:
        raise ValueError("empty server command")
    return parts[0], parts[1:]


def build_scenario(command: str, cases: list[dict[str, Any]]) -> dict[str, Any]:
    cmd, args = parse_command(command)
    steps = []
    for case in cases:
        steps.append(
            {
                "id": case["id"],
                "tool": case["tool"],
                "input": case.get("input", {}),
            }
        )

    return {
        "transport": "stdio",
        "command": cmd,
        "args": args,
        "steps": steps,
    }


def run_probe_script(
    probe_bin: str,
    scenario: dict[str, Any],
    report_out: Path,
    database_uri: str,
    timeout_ms: int,
    retries: int,
    retry_delay_ms: int,
) -> tuple[dict[str, Any], subprocess.CompletedProcess[str]]:
    report_out.parent.mkdir(parents=True, exist_ok=True)

    with tempfile.NamedTemporaryFile("w", suffix=".json", encoding="utf-8", delete=False) as tf:
        json.dump(scenario, tf, indent=2)
        scenario_path = Path(tf.name)

    env = os.environ.copy()
    env["MCP_PROBE_ALLOW_STDIO"] = "1"
    env["DATABASE_URI"] = database_uri
    env.setdefault("POSTGRES_MCP_STARTUP_DB_CONNECT", "warn")
    env.setdefault("POSTGRES_MCP_STARTUP_DB_CONNECT_TIMEOUT_SEC", "10")

    cmd = [
        probe_bin,
        "run",
        "--script",
        str(scenario_path),
        "--out",
        str(report_out),
        "--json",
        "--timeout-ms",
        str(timeout_ms),
        "--retries",
        str(retries),
        "--retry-delay-ms",
        str(retry_delay_ms),
    ]

    completed = subprocess.run(
        cmd,
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )

    try:
        report = load_json(report_out)
    except FileNotFoundError as exc:
        stderr = (completed.stderr or "").strip()
        raise RuntimeError(
            "mcp-probe did not produce a report file; "
            f"exit={completed.returncode}, stderr={stderr[:500]}"
        ) from exc
    finally:
        try:
            scenario_path.unlink(missing_ok=True)
        except OSError:
            pass

    return report, completed


def find_tool_steps(report: dict[str, Any]) -> dict[str, dict[str, Any]]:
    tool_steps: dict[str, dict[str, Any]] = {}
    for step in report.get("steps", []):
        name = step.get("name", "")
        if not isinstance(name, str):
            continue
        if not name.startswith("tool."):
            continue
        case_id = name[len("tool.") :]
        tool_steps[case_id] = step
    return tool_steps


def extract_first_text_content(value: Any) -> Any:
    if isinstance(value, str):
        return value

    if isinstance(value, dict):
        content = value.get("content")
        if isinstance(content, list):
            for block in content:
                if isinstance(block, dict) and block.get("type") == "text":
                    text = block.get("text")
                    if isinstance(text, str):
                        return text

    return value


def strip_error_prefix_if_present(value: Any, prefix: str) -> Any:
    if isinstance(value, str) and value.startswith(prefix):
        return value[len(prefix) :]
    return value


def parse_python_literal_if_possible(value: Any) -> Any:
    if not isinstance(value, str):
        return value

    try:
        return ast.literal_eval(value)
    except (SyntaxError, ValueError):
        return value


def normalize_whitespace(value: Any) -> Any:
    if isinstance(value, str):
        return " ".join(value.split())
    return value


def extract_structured_json(value: Any) -> Any:
    if isinstance(value, dict):
        structured = value.get("structuredContent")
        if structured is not None:
            return structured

    text_value = extract_first_text_content(value)
    if isinstance(text_value, str):
        try:
            return json.loads(text_value)
        except json.JSONDecodeError:
            return text_value

    return text_value


def normalize_object_key_order(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: normalize_object_key_order(value[k]) for k in sorted(value)}
    if isinstance(value, list):
        return [normalize_object_key_order(v) for v in value]
    return value


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def parse_ignore_descendant_keys(paths: list[str]) -> set[str]:
    keys: set[str] = set()
    for path in paths:
        if path.startswith("$..") and len(path) > 3:
            keys.add(path[3:])
    return keys


def apply_nondeterminism_rules(
    value: Any,
    ignore_descendant_keys: set[str],
    unordered_array_paths: set[str],
    path: str = "$",
) -> Any:
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        for key in sorted(value):
            if key in ignore_descendant_keys:
                continue
            child_path = f"{path}.{key}" if path != "$" else f"$.{key}"
            out[key] = apply_nondeterminism_rules(
                value[key],
                ignore_descendant_keys,
                unordered_array_paths,
                child_path,
            )
        return out

    if isinstance(value, list):
        out = [
            apply_nondeterminism_rules(
                item,
                ignore_descendant_keys,
                unordered_array_paths,
                f"{path}[{idx}]",
            )
            for idx, item in enumerate(value)
        ]
        if path in unordered_array_paths:
            out = sorted(out, key=canonical_json)
        return out

    return value


def apply_profile(
    actual: Any,
    profile_name: str,
    profiles: dict[str, Any],
    python_prefix: str,
) -> Any:
    profile = profiles.get(profile_name)
    if not isinstance(profile, dict):
        raise ValueError(f"unknown normalization profile: {profile_name}")

    steps = profile.get("steps", [])
    value = copy.deepcopy(actual)
    for step in steps:
        if step == "extract_first_text_content":
            value = extract_first_text_content(value)
        elif step == "strip_error_prefix_if_present":
            value = strip_error_prefix_if_present(value, python_prefix)
        elif step == "parse_python_literal_if_possible":
            value = parse_python_literal_if_possible(value)
        elif step == "normalize_object_key_order":
            value = normalize_object_key_order(value)
        elif step == "normalize_whitespace":
            value = normalize_whitespace(value)
        elif step == "extract_structured_json":
            value = extract_structured_json(value)
        else:
            raise ValueError(f"unsupported normalization step: {step}")

    return value


def classify_tool_call_error(error_message: str | None) -> str | None:
    if not error_message:
        return None

    msg = error_message.lower()
    if "-32602" in msg or "invalid params" in msg or "invalid_params" in msg:
        return "invalid_params"
    if "-32603" in msg or "internal" in msg:
        return "internal_error"
    if "-32000" in msg:
        return "server_error"
    return None


def extract_error_message_from_actual(actual: Any, rust_error_key: str) -> str:
    if isinstance(actual, dict):
        structured = actual.get("structuredContent")
        if isinstance(structured, dict):
            err_value = structured.get(rust_error_key)
            if isinstance(err_value, str):
                return err_value

        content = actual.get("content")
        if isinstance(content, list):
            for block in content:
                if not isinstance(block, dict):
                    continue
                if block.get("type") != "text":
                    continue
                text = block.get("text")
                if isinstance(text, str):
                    # Rust often wraps errors in JSON text payloads.
                    try:
                        parsed = json.loads(text)
                    except json.JSONDecodeError:
                        return text
                    if isinstance(parsed, dict):
                        err_value = parsed.get(rust_error_key)
                        if isinstance(err_value, str):
                            return err_value
                    return text

    return str(actual)


def normalize_case_result(
    step: dict[str, Any] | None,
    case: dict[str, Any],
    impl: str,
    profiles: dict[str, Any],
    error_mapping: dict[str, Any],
    ignore_descendant_keys: set[str],
    unordered_array_paths: set[str],
) -> NormalizedResult:
    if step is None:
        return NormalizedResult(
            kind="tool_call_error",
            error_message="missing tool step in probe report",
            error_class="missing_step",
        )

    data = step.get("data") if isinstance(step.get("data"), dict) else {}
    actual = data.get("actual")

    raw_status = step.get("status") if isinstance(step.get("status"), str) else None
    raw_detail = step.get("detail") if isinstance(step.get("detail"), str) else None

    if actual is None:
        err_obj = data.get("error") if isinstance(data.get("error"), dict) else {}
        error_message = err_obj.get("message") if isinstance(err_obj.get("message"), str) else None
        if error_message is None:
            error_message = raw_detail or "tool call failed"

        return NormalizedResult(
            kind="tool_call_error",
            error_message=error_message,
            error_class=classify_tool_call_error(error_message),
            raw_step_status=raw_status,
            raw_step_detail=raw_detail,
        )

    is_error = isinstance(actual, dict) and bool(actual.get("isError"))
    profile_name = case["normalization_profiles"][impl]
    python_prefix = str(error_mapping.get("python_prefix", "Error: "))
    rust_error_key = str(error_mapping.get("rust_error_object_key", "error"))

    normalized = apply_profile(actual, profile_name, profiles, python_prefix)

    if not is_error:
        if isinstance(normalized, str) and normalized.startswith(python_prefix):
            is_error = True
        elif isinstance(normalized, dict):
            maybe_error = normalized.get(rust_error_key)
            if isinstance(maybe_error, str):
                is_error = True

    if is_error:
        message: str
        if isinstance(normalized, str):
            message = normalized
        elif isinstance(normalized, dict):
            maybe = normalized.get(rust_error_key)
            if isinstance(maybe, str):
                message = maybe
            else:
                message = extract_error_message_from_actual(actual, rust_error_key)
        else:
            message = extract_error_message_from_actual(actual, rust_error_key)

        return NormalizedResult(
            kind="error",
            error_message=normalize_whitespace(message),
            raw_step_status=raw_status,
            raw_step_detail=raw_detail,
        )

    payload = normalized
    if isinstance(payload, (dict, list)):
        payload = apply_nondeterminism_rules(
            payload,
            ignore_descendant_keys,
            unordered_array_paths,
        )
        payload = normalize_object_key_order(payload)

    return NormalizedResult(
        kind="ok",
        payload=payload,
        raw_step_status=raw_status,
        raw_step_detail=raw_detail,
    )


def check_payload_shape(payload: Any, shape: dict[str, Any]) -> list[str]:
    failures: list[str] = []

    root = shape.get("root")
    if root == "array<object>":
        if not isinstance(payload, list):
            failures.append("payload root mismatch: expected array<object>")
            return failures
        if any(not isinstance(item, dict) for item in payload):
            failures.append("payload root mismatch: expected array<object> items")
            return failures
    elif root == "object":
        if not isinstance(payload, dict):
            failures.append("payload root mismatch: expected object")
            return failures
    elif root == "string":
        if not isinstance(payload, str):
            failures.append("payload root mismatch: expected string")
            return failures
    elif root is not None:
        failures.append(f"unsupported payload root assertion: {root}")
        return failures

    required_keys = shape.get("required_keys")
    if isinstance(required_keys, list) and required_keys:
        if isinstance(payload, dict):
            for key in required_keys:
                if key not in payload:
                    failures.append(f"payload missing required key: {key}")
        elif isinstance(payload, list):
            # For empty arrays we treat schema key checks as not-applicable.
            for idx, item in enumerate(payload):
                if not isinstance(item, dict):
                    failures.append(f"payload[{idx}] is not an object")
                    continue
                for key in required_keys:
                    if key not in item:
                        failures.append(f"payload[{idx}] missing required key: {key}")

    exact = shape.get("exact")
    if exact is not None:
        if payload != exact:
            failures.append(f"payload exact mismatch: expected {exact!r}")

    contains_any = shape.get("contains_any")
    if isinstance(contains_any, list):
        if not isinstance(payload, str):
            failures.append("payload contains_any requires string payload")
        elif not any(isinstance(s, str) and s in payload for s in contains_any):
            failures.append(f"payload missing required substrings: {contains_any!r}")

    return failures


def check_expected(result: NormalizedResult, expected: dict[str, Any]) -> list[str]:
    failures: list[str] = []

    expected_kind = expected.get("kind")
    if expected_kind and result.kind != expected_kind:
        failures.append(f"kind mismatch: expected {expected_kind}, got {result.kind}")

    expected_error_class = expected.get("error_class")
    if expected_error_class and result.error_class != expected_error_class:
        failures.append(
            f"error_class mismatch: expected {expected_error_class}, got {result.error_class}"
        )

    error_text_exact = expected.get("error_text_exact")
    if error_text_exact is not None:
        if result.error_message != error_text_exact:
            failures.append(
                "error_text_exact mismatch: "
                f"expected {error_text_exact!r}, got {result.error_message!r}"
            )

    error_text_contains = expected.get("error_text_contains")
    if error_text_contains is not None:
        if not isinstance(result.error_message, str) or error_text_contains not in result.error_message:
            failures.append(
                "error_text_contains mismatch: "
                f"expected substring {error_text_contains!r}, got {result.error_message!r}"
            )

    payload_shape = expected.get("payload_shape")
    if isinstance(payload_shape, dict):
        failures.extend(check_payload_shape(result.payload, payload_shape))

    return failures


def canonical_for_compare(result: NormalizedResult) -> dict[str, Any]:
    out: dict[str, Any] = {"kind": result.kind}
    if result.kind == "ok":
        out["payload"] = result.payload
    else:
        out["error_message"] = result.error_message
        if result.error_class is not None:
            out["error_class"] = result.error_class
    return out


def diff_values(
    expected: Any,
    actual: Any,
    path: str,
    diffs: list[dict[str, Any]],
    abs_tol: float,
    max_entries: int,
) -> None:
    if len(diffs) >= max_entries:
        return

    if isinstance(expected, (int, float)) and isinstance(actual, (int, float)):
        if abs(float(expected) - float(actual)) <= abs_tol:
            return

    if type(expected) is not type(actual):
        diffs.append({"path": path, "expected": expected, "actual": actual})
        return

    if isinstance(expected, dict):
        keys = sorted(set(expected.keys()) | set(actual.keys()))
        for key in keys:
            child_path = f"{path}.{key}" if path != "$" else f"$.{key}"
            if key not in expected:
                diffs.append({"path": child_path, "expected": "<missing>", "actual": actual[key]})
                if len(diffs) >= max_entries:
                    return
                continue
            if key not in actual:
                diffs.append({"path": child_path, "expected": expected[key], "actual": "<missing>"})
                if len(diffs) >= max_entries:
                    return
                continue
            diff_values(expected[key], actual[key], child_path, diffs, abs_tol, max_entries)
            if len(diffs) >= max_entries:
                return
        return

    if isinstance(expected, list):
        if len(expected) != len(actual):
            diffs.append(
                {
                    "path": path,
                    "expected": f"len={len(expected)}",
                    "actual": f"len={len(actual)}",
                }
            )
            if len(diffs) >= max_entries:
                return
        for idx, (exp_item, act_item) in enumerate(zip(expected, actual)):
            child_path = f"{path}[{idx}]"
            diff_values(exp_item, act_item, child_path, diffs, abs_tol, max_entries)
            if len(diffs) >= max_entries:
                return
        return

    if expected != actual:
        diffs.append({"path": path, "expected": expected, "actual": actual})


def _run_psql_sql(
    host: str,
    port: str,
    user: str,
    database: str,
    password: str,
    sql: str,
    *,
    on_error_stop: bool = True,
    tuples_only: bool = False,
) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["PGPASSWORD"] = password

    cmd = [
        "psql",
        "-h",
        host,
        "-p",
        port,
        "-U",
        user,
        "-d",
        database,
        "-q",
    ]
    if on_error_stop:
        cmd.extend(["-v", "ON_ERROR_STOP=1"])
    if tuples_only:
        cmd.extend(["-A", "-t"])
    cmd.extend(["-c", sql])

    return subprocess.run(cmd, check=False, capture_output=True, text=True, env=env)


def _psql_bool(
    host: str,
    port: str,
    user: str,
    database: str,
    password: str,
    sql: str,
) -> tuple[bool | None, str | None]:
    completed = _run_psql_sql(
        host,
        port,
        user,
        database,
        password,
        sql,
        on_error_stop=True,
        tuples_only=True,
    )
    if completed.returncode != 0:
        return None, (completed.stderr or "").strip()
    value = (completed.stdout or "").strip().lower()
    return value in {"t", "true", "1", "on"}, None


def seed_local_fixture_data(database_uri: str) -> dict[str, Any]:
    parsed = urlparse(database_uri)
    if parsed.scheme not in {"postgres", "postgresql", "postgresql+psycopg"}:
        raise RuntimeError(f"unsupported database URI scheme for seeding: {parsed.scheme}")

    database = parsed.path.lstrip("/") or "postgres"
    host = parsed.hostname or "127.0.0.1"
    port = str(parsed.port or 5432)
    user = parsed.username or "postgres"
    password = unquote(parsed.password or "")

    seed: dict[str, Any] = {
        "base_fixture_seeded": False,
        "extensions": {},
        "warnings": [],
    }

    base_sql = """
CREATE TABLE IF NOT EXISTS public.users (
  id integer PRIMARY KEY,
  name text
);
INSERT INTO public.users (id, name)
VALUES (1, 'parity-user')
ON CONFLICT (id) DO NOTHING;
""".strip()

    completed = _run_psql_sql(
        host,
        port,
        user,
        database,
        password,
        base_sql,
        on_error_stop=True,
        tuples_only=False,
    )
    if completed.returncode != 0:
        stderr = (completed.stderr or "").strip()
        raise RuntimeError(f"failed to seed parity fixture data via psql: {stderr[:500]}")
    seed["base_fixture_seeded"] = True

    for extension_name in ("pg_stat_statements", "hypopg"):
        ext_info: dict[str, Any] = {
            "requested": True,
            "available": False,
            "created_or_exists": False,
            "active": False,
        }

        available, available_err = _psql_bool(
            host,
            port,
            user,
            database,
            password,
            f"SELECT EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = '{extension_name}');",
        )
        if available is None:
            ext_info["availability_error"] = available_err
            seed["warnings"].append(
                f"could not determine extension availability for {extension_name}: {available_err}"
            )
            seed["extensions"][extension_name] = ext_info
            continue

        ext_info["available"] = available
        if not available:
            seed["warnings"].append(
                f"extension not available on server image: {extension_name}"
            )
            seed["extensions"][extension_name] = ext_info
            continue

        create_stmt = f"CREATE EXTENSION IF NOT EXISTS {extension_name};"
        create_result = _run_psql_sql(
            host,
            port,
            user,
            database,
            password,
            create_stmt,
            on_error_stop=True,
            tuples_only=False,
        )
        if create_result.returncode != 0:
            err = (create_result.stderr or "").strip()
            ext_info["create_error"] = err
            seed["warnings"].append(
                f"failed to create extension {extension_name}: {err[:240]}"
            )
            seed["extensions"][extension_name] = ext_info
            continue

        ext_info["created_or_exists"] = True

        if extension_name == "pg_stat_statements":
            probe_sql = "SELECT COUNT(*) FROM pg_stat_statements;"
            probe = _run_psql_sql(
                host,
                port,
                user,
                database,
                password,
                probe_sql,
                on_error_stop=True,
                tuples_only=True,
            )
            if probe.returncode == 0:
                ext_info["active"] = True
                value = (probe.stdout or "").strip()
                ext_info["row_count"] = int(value) if value.isdigit() else value

                # Generate a little activity so top-query paths have data.
                _run_psql_sql(
                    host,
                    port,
                    user,
                    database,
                    password,
                    "SELECT * FROM public.users WHERE id = 1;",
                    on_error_stop=False,
                    tuples_only=False,
                )
                _run_psql_sql(
                    host,
                    port,
                    user,
                    database,
                    password,
                    "SELECT COUNT(*) FROM public.users;",
                    on_error_stop=False,
                    tuples_only=False,
                )
            else:
                err = (probe.stderr or "").strip()
                ext_info["probe_error"] = err
                seed["warnings"].append(
                    "pg_stat_statements created but not queryable "
                    f"(likely missing shared_preload_libraries): {err[:240]}"
                )
        else:
            active, active_err = _psql_bool(
                host,
                port,
                user,
                database,
                password,
                "SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'hypopg');",
            )
            if active is None:
                ext_info["probe_error"] = active_err
                seed["warnings"].append(
                    f"could not verify hypopg activation: {active_err}"
                )
            else:
                ext_info["active"] = active

        seed["extensions"][extension_name] = ext_info

    return seed


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run semantic parity harness for python<->rust postgres MCP")

    parser.add_argument(
        "--manifest",
        default="fixtures/parity_v2/manifest.json",
        help="Path to parity manifest JSON (default: fixtures/parity_v2/manifest.json)",
    )
    parser.add_argument(
        "--probe-bin",
        default=None,
        help="Path to rust mcp-probe binary (default: ../../tools/mcp-probe/rust/target/release/mcp-probe)",
    )
    parser.add_argument(
        "--python-cmd",
        default=os.getenv("PARITY_PYTHON_SERVER_CMD"),
        help="Python server launch command (shell-style string).",
    )
    parser.add_argument(
        "--rust-cmd",
        default=os.getenv("PARITY_RUST_SERVER_CMD"),
        help="Rust server launch command (shell-style string).",
    )
    parser.add_argument(
        "--database-uri",
        default=None,
        help="Database URI (default resolves from DATABASE_URI, DATABASE_URL, POSTGRES_* or local smoke DB)",
    )
    parser.add_argument(
        "--out-dir",
        default=".tmp/parity_v2",
        help="Output directory for reports (default: .tmp/parity_v2)",
    )
    parser.add_argument(
        "--timeout-ms",
        type=int,
        default=15000,
        help="Per-step timeout for probe runs (default: 15000)",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=0,
        help="Retry count for probe runs (default: 0)",
    )
    parser.add_argument(
        "--retry-delay-ms",
        type=int,
        default=250,
        help="Retry delay for probe runs (default: 250)",
    )
    parser.add_argument(
        "--max-diff-entries",
        type=int,
        default=25,
        help="Maximum semantic diff entries recorded per case (default: 25)",
    )
    parser.add_argument(
        "--no-seed",
        action="store_true",
        help="Skip fixture seeding SQL (base table + extension setup checks).",
    )

    return parser.parse_args()


def main() -> int:
    args = parse_args()

    root = Path(__file__).resolve().parents[1]
    manifest_path = (root / args.manifest).resolve()
    manifest = load_json(manifest_path)

    fixtures_path = (root / manifest["fixtures_file"]).resolve()
    rules_path = (root / manifest["normalization_rules_file"]).resolve()
    known_path = (root / manifest["known_differences_file"]).resolve()

    cases_doc = load_json(fixtures_path)
    rules_doc = load_json(rules_path)
    known_doc = load_json(known_path)

    cases = cases_doc.get("cases", [])
    profiles = rules_doc.get("profiles", {})
    error_mapping = rules_doc.get("error_mapping", {})

    nondeterminism = rules_doc.get("nondeterminism", {})
    numeric_tolerance = float(
        ((nondeterminism.get("numeric_tolerance") or {}).get("default_abs") or 0.0)
    )
    ignore_descendant_keys = parse_ignore_descendant_keys(
        list(nondeterminism.get("ignore_paths", []))
    )
    unordered_array_paths = set(nondeterminism.get("unordered_array_paths", []))

    known_by_id = {
        entry["id"]: entry for entry in known_doc.get("differences", []) if isinstance(entry, dict) and "id" in entry
    }

    probe_bin = args.probe_bin
    if not probe_bin:
        probe_bin = str((root / "../../tools/mcp-probe/rust/target/release/mcp-probe").resolve())
    if not Path(probe_bin).exists():
        print(f"missing probe binary: {probe_bin}", file=sys.stderr)
        return 2

    rust_cmd = args.rust_cmd or str((root / "target/debug/postgres-mcp").resolve())

    python_cmd = args.python_cmd
    if not python_cmd:
        default_venv_cmd = Path("/tmp/postgres-mcp-parity-venv/bin/postgres-mcp")
        if default_venv_cmd.exists():
            python_cmd = str(default_venv_cmd)
        else:
            python_cmd = "postgres-mcp"

    database_uri = resolve_database_uri(args.database_uri)

    seed_info: dict[str, Any] = {"skipped": True}
    if not args.no_seed:
        seed_info = seed_local_fixture_data(database_uri)

    out_dir = (root / args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    python_scenario = build_scenario(python_cmd, cases)
    rust_scenario = build_scenario(rust_cmd, cases)

    python_report_path = out_dir / "probe_python_report.json"
    rust_report_path = out_dir / "probe_rust_report.json"

    python_report, python_probe_proc = run_probe_script(
        probe_bin,
        python_scenario,
        python_report_path,
        database_uri,
        args.timeout_ms,
        args.retries,
        args.retry_delay_ms,
    )
    rust_report, rust_probe_proc = run_probe_script(
        probe_bin,
        rust_scenario,
        rust_report_path,
        database_uri,
        args.timeout_ms,
        args.retries,
        args.retry_delay_ms,
    )

    python_steps = find_tool_steps(python_report)
    rust_steps = find_tool_steps(rust_report)

    case_results: list[dict[str, Any]] = []
    failed_cases = 0

    for case in cases:
        case_id = case["id"]
        py_step = python_steps.get(case_id)
        rs_step = rust_steps.get(case_id)

        py_norm = normalize_case_result(
            py_step,
            case,
            "python",
            profiles,
            error_mapping,
            ignore_descendant_keys,
            unordered_array_paths,
        )
        rs_norm = normalize_case_result(
            rs_step,
            case,
            "rust",
            profiles,
            error_mapping,
            ignore_descendant_keys,
            unordered_array_paths,
        )

        py_expected = case.get("python_expected", {})
        rs_expected = case.get("rust_expected", {})

        py_failures = check_expected(py_norm, py_expected)
        rs_failures = check_expected(rs_norm, rs_expected)

        py_canonical = canonical_for_compare(py_norm)
        rs_canonical = canonical_for_compare(rs_norm)

        semantic_diff: list[dict[str, Any]] = []
        diff_values(
            py_canonical,
            rs_canonical,
            "$",
            semantic_diff,
            numeric_tolerance,
            args.max_diff_entries,
        )

        mode = case.get("comparison_mode")
        semantic_failures: list[str] = []
        if mode == "equivalent" and semantic_diff:
            semantic_failures.append(
                "equivalent case has semantic diff after normalization"
            )

        failures = py_failures + rs_failures + semantic_failures

        known_difference_ids = list(case.get("known_difference_ids", []))
        unknown_known_diff_ids = [
            kd for kd in known_difference_ids if kd not in known_by_id
        ]
        if unknown_known_diff_ids:
            failures.append(
                f"unknown known_difference_ids in case: {unknown_known_diff_ids}"
            )

        status = "pass" if not failures else "fail"
        if status == "fail":
            failed_cases += 1

        case_results.append(
            {
                "id": case_id,
                "tool": case.get("tool"),
                "comparison_mode": mode,
                "known_difference_ids": known_difference_ids,
                "status": status,
                "failures": failures,
                "python": {
                    "normalized": py_canonical,
                    "expectation_failures": py_failures,
                    "probe_step_status": py_norm.raw_step_status,
                    "probe_step_detail": py_norm.raw_step_detail,
                },
                "rust": {
                    "normalized": rs_canonical,
                    "expectation_failures": rs_failures,
                    "probe_step_status": rs_norm.raw_step_status,
                    "probe_step_detail": rs_norm.raw_step_detail,
                },
                "semantic_diff": semantic_diff,
                "semantic_diff_count": len(semantic_diff),
            }
        )

    summary = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "manifest": str(manifest_path),
        "fixtures": str(fixtures_path),
        "normalization_rules": str(rules_path),
        "known_differences": str(known_path),
        "probe_bin": probe_bin,
        "python_cmd": python_cmd,
        "rust_cmd": rust_cmd,
        "database_uri": redact_database_uri(database_uri),
        "probe_runs": {
            "python_exit_code": python_probe_proc.returncode,
            "rust_exit_code": rust_probe_proc.returncode,
        },
        "seed": seed_info,
        "cases_total": len(case_results),
        "cases_failed": failed_cases,
        "cases_passed": len(case_results) - failed_cases,
    }

    output = {
        "summary": summary,
        "cases": case_results,
    }

    report_path = out_dir / "semantic_parity_report.json"
    with report_path.open("w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=True)
        f.write("\n")

    print(
        "semantic parity report: "
        f"passed={summary['cases_passed']} failed={summary['cases_failed']} total={summary['cases_total']}"
    )
    print(f"report path: {report_path}")
    for warning in seed_info.get("warnings", []):
        print(f"seed warning: {warning}")

    if failed_cases:
        print("failed cases:")
        for case in case_results:
            if case["status"] != "fail":
                continue
            reason = "; ".join(case["failures"][:3])
            print(f"- {case['id']}: {reason}")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
