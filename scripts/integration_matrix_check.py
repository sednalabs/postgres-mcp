#!/usr/bin/env python3
"""Integration matrix harness for postgres-mcp.

Runs a scenario matrix across Postgres versions, extension states, and failure
modes via rust mcp-probe stdio scripted runs.

Release gate behavior:
- Fails when any scenario at/above --fail-on severity fails.
- Fails when required targets are missing.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shlex
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, quote, unquote, urlencode, urlparse, urlunparse

SEVERITY_ORDER = {"low": 0, "medium": 1, "high": 2}

MATRIX_DEFAULT_URIS = {
    "MATRIX_DB_URI_PG15_FULL": "postgresql://matrix_user:matrix_pass@127.0.0.1:55415/matrix_db",
    "MATRIX_DB_URI_PG18_FULL": "postgresql://matrix_user:matrix_pass@127.0.0.1:55418/matrix_db",
    "MATRIX_DB_URI_PG18_FULL_LIMITED": "postgresql://matrix_limited:matrix_limited_pass@127.0.0.1:55418/matrix_db",
    "MATRIX_DB_URI_PG18_MISSING_EXT": "postgresql://matrix_user:matrix_pass@127.0.0.1:55419/matrix_db",
    "MATRIX_DB_URI_PG18_PGSTAT_DEGRADED": "postgresql://matrix_user:matrix_pass@127.0.0.1:55420/matrix_db",
}

TRUTHY_VALUES = {"1", "true", "yes", "on"}
DOCKER_UNAVAILABLE_MARKERS = (
    "permission denied while trying to connect to the docker daemon socket",
    "cannot connect to the docker daemon",
    "dial unix /var/run/docker.sock: connect: permission denied",
    "docker is required",
    "docker: command not found",
    "snap-confine is packaged without necessary permissions",
)
COMPOSE_BUILD_POLICIES = {"auto", "always", "never"}


@dataclass
class Target:
    id: str
    label: str
    postgres_version: str
    extension_state: str
    tags: set[str]
    required: bool
    database_uri: str | None
    database_uri_env: str | None


@dataclass
class ProbeRun:
    probe_exit_code: int
    step_status: str | None
    step_detail: str | None
    payload: Any
    raw_actual: Any



def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)



def normalize_database_uri(uri: str) -> str:
    if uri.startswith("postgresql+psycopg://"):
        return "postgresql://" + uri[len("postgresql+psycopg://") :]
    return uri



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



def parse_command(command: str) -> tuple[str, list[str]]:
    parts = shlex.split(command)
    if not parts:
        raise ValueError("empty server command")
    return parts[0], parts[1:]



def run_cmd(cmd: list[str], *, env: dict[str, str] | None = None, cwd: Path | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        cmd,
        check=False,
        capture_output=True,
        text=True,
        env=env,
        cwd=str(cwd) if cwd else None,
    )


def env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw.strip())
    except ValueError:
        return default


def hash_file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            digest.update(chunk)
    return digest.hexdigest()


def hash_compose_build_inputs(compose_file: Path) -> tuple[str, str]:
    compose_file_sha256 = hash_file_sha256(compose_file)
    digest = hashlib.sha256()
    digest.update(f"compose_file:{compose_file_sha256}\n".encode("utf-8"))

    compose_root = compose_file.parent
    for rel_dir in ("images", "initdb"):
        root = compose_root / rel_dir
        if not root.exists():
            continue
        files = sorted(
            (path for path in root.rglob("*") if path.is_file()),
            key=lambda path: path.relative_to(compose_root).as_posix(),
        )
        for path in files:
            rel = path.relative_to(compose_root).as_posix()
            digest.update(rel.encode("utf-8"))
            digest.update(b"\0")
            digest.update(hash_file_sha256(path).encode("ascii"))
            digest.update(b"\n")

    return compose_file_sha256, digest.hexdigest()


def compose_build_state_path(out_dir: Path) -> Path:
    return out_dir / "compose_build_state.json"


def load_compose_build_state(path: Path) -> dict[str, Any]:
    try:
        data = load_json(path)
        if isinstance(data, dict):
            return data
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError:
        return {}
    return {}


def save_compose_build_state(path: Path, state: dict[str, Any]) -> None:
    with path.open("w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, ensure_ascii=True)
        f.write("\n")


def resolve_compose_build(
    *,
    policy: str,
    compose_file: Path,
    compose_project: str,
    docker_cmd: str,
    out_dir: Path,
) -> tuple[bool, str, str, str]:
    if not compose_file.exists():
        return True, "compose_file_missing", "", ""

    compose_file_sha256, compose_inputs_sha256 = hash_compose_build_inputs(compose_file)
    if policy == "always":
        return True, "policy_always", compose_file_sha256, compose_inputs_sha256
    if policy == "never":
        return False, "policy_never", compose_file_sha256, compose_inputs_sha256

    state = load_compose_build_state(compose_build_state_path(out_dir))
    if (
        state.get("compose_file_sha256") == compose_file_sha256
        and state.get("compose_inputs_sha256") == compose_inputs_sha256
        and state.get("compose_project") == compose_project
        and state.get("docker_cmd") == docker_cmd
    ):
        return False, "auto_reuse", compose_file_sha256, compose_inputs_sha256
    return True, "auto_rebuild", compose_file_sha256, compose_inputs_sha256


def classify_failure_messages(failures: list[str]) -> str:
    text = "\n".join(str(item) for item in failures).lower()
    if (
        "probe execution failed" in text
        or "timeout" in text
        or "timed out" in text
        or "docker" in text
        or "connection refused" in text
    ):
        return "infra_error"
    if (
        "mismatch" in text
        or "expected" in text
        or "missing expected" in text
        or "required meta key missing" in text
    ):
        return "contract_mismatch"
    if (
        "scenario missing" in text
        or "scenario input must" in text
        or "required target" in text
        or "resolved no available targets" in text
    ):
        return "config_error"
    return "assertion_failure"


def summarize_top_failures(results: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    if limit <= 0:
        return []
    top: list[dict[str, Any]] = []
    for item in results:
        failures_raw = item.get("failures")
        failures = failures_raw if isinstance(failures_raw, list) else []
        message = str(failures[0]) if failures else "unknown failure"
        top.append(
            {
                "scenario_id": str(item.get("scenario_id", "unknown")),
                "target_id": item.get("target_id"),
                "severity": str(item.get("severity", "high")),
                "failure_class": str(item.get("failure_class", "assertion_failure")),
                "message": message,
            }
        )
        if len(top) >= limit:
            break
    return top



def apply_compose_default_uris() -> None:
    for key, value in MATRIX_DEFAULT_URIS.items():
        os.environ.setdefault(key, value)


def env_truthy(name: str) -> bool:
    value = os.getenv(name, "")
    return value.strip().lower() in TRUTHY_VALUES


def should_allow_compose_unavailable_skip() -> bool:
    override = os.getenv("INTEGRATION_MATRIX_ALLOW_COMPOSE_UNAVAILABLE")
    if override is not None:
        return override.strip().lower() in TRUTHY_VALUES
    return not env_truthy("CI")


def is_compose_unavailable_error(text: str) -> bool:
    normalized = text.lower()
    return any(marker in normalized for marker in DOCKER_UNAVAILABLE_MARKERS)



def build_target(entry: dict[str, Any]) -> Target:
    target_id = str(entry.get("id", "")).strip()
    if not target_id:
        raise ValueError("target id must be non-empty")

    env_name = entry.get("database_uri_env")
    if env_name is not None and not isinstance(env_name, str):
        raise ValueError(f"target {target_id}: database_uri_env must be string")

    direct_uri = entry.get("database_uri")
    if direct_uri is not None and not isinstance(direct_uri, str):
        raise ValueError(f"target {target_id}: database_uri must be string")

    uri: str | None = None
    if isinstance(direct_uri, str) and direct_uri.strip():
        uri = normalize_database_uri(direct_uri.strip())
    elif isinstance(env_name, str) and env_name.strip():
        env_uri = os.getenv(env_name.strip())
        if env_uri:
            uri = normalize_database_uri(env_uri)

    tags_raw = entry.get("tags", [])
    if not isinstance(tags_raw, list):
        raise ValueError(f"target {target_id}: tags must be array")
    tags = {str(tag).strip() for tag in tags_raw if str(tag).strip()}

    return Target(
        id=target_id,
        label=str(entry.get("label", target_id)),
        postgres_version=str(entry.get("postgres_version", "unknown")),
        extension_state=str(entry.get("extension_state", "unknown")),
        tags=tags,
        required=bool(entry.get("required", False)),
        database_uri=uri,
        database_uri_env=env_name.strip() if isinstance(env_name, str) and env_name.strip() else None,
    )



def matches_target_selector(scenario: dict[str, Any], target: Target) -> bool:
    target_ids = scenario.get("target_ids")
    if isinstance(target_ids, list) and target_ids:
        ids = {str(value) for value in target_ids}
        if target.id not in ids:
            return False

    tags_all = scenario.get("target_tags_all")
    if isinstance(tags_all, list) and tags_all:
        needed = {str(value) for value in tags_all}
        if not needed.issubset(target.tags):
            return False

    tags_any = scenario.get("target_tags_any")
    if isinstance(tags_any, list) and tags_any:
        any_tags = {str(value) for value in tags_any}
        if target.tags.isdisjoint(any_tags):
            return False

    return True



def mutate_database_uri(uri: str, mutation: str) -> str:
    parsed = urlparse(uri)
    scheme = parsed.scheme
    if scheme not in {"postgres", "postgresql", "postgresql+psycopg"}:
        return uri

    username = parsed.username or ""
    password = unquote(parsed.password or "")
    host = parsed.hostname or "127.0.0.1"
    port = parsed.port

    query = dict(parse_qsl(parsed.query, keep_blank_values=True))

    if mutation == "bad_password":
        password = "invalid_matrix_password"
    elif mutation == "network_timeout":
        host = "10.255.255.1"
        query["connect_timeout"] = "1"
    elif mutation == "closed_port":
        host = "127.0.0.1"
        port = 65432
        query["connect_timeout"] = "1"
    else:
        raise ValueError(f"unsupported uri_mutation: {mutation}")

    user_enc = quote(username, safe="")
    pass_enc = quote(password, safe="")
    auth = f"{user_enc}:{pass_enc}@" if username else ""
    host_part = host
    port_part = f":{port}" if port else ""
    netloc = f"{auth}{host_part}{port_part}"
    query_str = urlencode(query)

    return urlunparse(
        (
            parsed.scheme,
            netloc,
            parsed.path,
            parsed.params,
            query_str,
            parsed.fragment,
        )
    )



def build_probe_scenario(command: str, tool_name: str, tool_input: dict[str, Any]) -> dict[str, Any]:
    cmd, args = parse_command(command)
    return {
        "transport": "stdio",
        "command": cmd,
        "args": args,
        "steps": [
            {
                "id": "matrix_case",
                "tool": tool_name,
                "input": tool_input,
            }
        ],
    }



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



def extract_payload(actual: Any) -> Any:
    def unwrap_contract_payload(value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        ok = value.get("ok")
        # Keep v2 success envelopes intact so expectation checks can validate
        # payload.meta / payload.data / payload.ok consistently.
        if ok is True:
            return value
        if ok is False and "error" in value:
            return value.get("error")
        return value

    if isinstance(actual, dict):
        structured = actual.get("structuredContent")
        if structured is not None:
            return unwrap_contract_payload(structured)

    text_value = extract_first_text_content(actual)
    if isinstance(text_value, str):
        try:
            return unwrap_contract_payload(json.loads(text_value))
        except json.JSONDecodeError:
            return text_value

    return unwrap_contract_payload(text_value)



def extract_error_and_message_text(payload: Any, step_detail: str | None) -> tuple[str, str]:
    error_parts: list[str] = []
    message_parts: list[str] = []

    if isinstance(payload, dict):
        maybe_error = payload.get("error")
        if isinstance(maybe_error, str):
            error_parts.append(maybe_error)
        maybe_message = payload.get("message")
        if isinstance(maybe_message, str):
            message_parts.append(maybe_message)
            error_parts.append(maybe_message)
    elif isinstance(payload, str):
        error_parts.append(payload)

    if isinstance(step_detail, str) and step_detail:
        error_parts.append(step_detail)

    return "\n".join(error_parts), "\n".join(message_parts)



def find_tool_step(report: dict[str, Any]) -> dict[str, Any] | None:
    for step in report.get("steps", []):
        if step.get("name") == "tool.matrix_case":
            return step
    return None



def run_probe(
    *,
    probe_bin: str,
    rust_cmd: str,
    tool_name: str,
    tool_input: dict[str, Any],
    database_uri: str,
    out_dir: Path,
    report_prefix: str,
    timeout_ms: int,
    retries: int,
    retry_delay_ms: int,
) -> ProbeRun:
    report_path = out_dir / f"{report_prefix}.probe.json"
    scenario = build_probe_scenario(rust_cmd, tool_name, tool_input)

    with tempfile.NamedTemporaryFile("w", suffix=".json", encoding="utf-8", delete=False) as tf:
        json.dump(scenario, tf, indent=2)
        scenario_path = Path(tf.name)

    env = os.environ.copy()
    env["MCP_PROBE_ALLOW_STDIO"] = "1"
    env["DATABASE_URI"] = database_uri
    env.setdefault("POSTGRES_MCP_STARTUP_DB_CONNECT", "warn")
    env.setdefault("POSTGRES_MCP_STARTUP_DB_CONNECT_TIMEOUT_SEC", "3")

    cmd = [
        probe_bin,
        "run",
        "--script",
        str(scenario_path),
        "--out",
        str(report_path),
        "--json",
        "--timeout-ms",
        str(timeout_ms),
        "--retries",
        str(retries),
        "--retry-delay-ms",
        str(retry_delay_ms),
    ]

    completed = run_cmd(cmd, env=env)

    try:
        report = load_json(report_path)
    except FileNotFoundError as exc:
        raise RuntimeError(
            "mcp-probe did not produce a report file "
            f"(exit={completed.returncode}, stderr={(completed.stderr or '').strip()[:400]})"
        ) from exc
    finally:
        try:
            scenario_path.unlink(missing_ok=True)
        except OSError:
            pass

    step = find_tool_step(report)
    if not isinstance(step, dict):
        raise RuntimeError("probe report did not include tool.matrix_case step")

    data = step.get("data") if isinstance(step.get("data"), dict) else {}
    actual = data.get("actual")
    payload = extract_payload(actual)

    return ProbeRun(
        probe_exit_code=completed.returncode,
        step_status=step.get("status") if isinstance(step.get("status"), str) else None,
        step_detail=step.get("detail") if isinstance(step.get("detail"), str) else None,
        payload=payload,
        raw_actual=actual,
    )



def redact_forbidden_tokens(uri: str) -> list[str]:
    parsed = urlparse(uri)
    tokens: list[str] = []

    if parsed.password:
        tokens.append(parsed.password)
        tokens.append(unquote(parsed.password))
    if parsed.username:
        tokens.append(parsed.username)

    # URI-level leakage should never happen in tool errors.
    tokens.append(uri)

    normalized: list[str] = []
    seen: set[str] = set()
    for token in tokens:
        token = str(token).strip()
        if not token:
            continue
        if token in seen:
            continue
        seen.add(token)
        normalized.append(token)
    return normalized



def expect_list(value: Any) -> bool:
    return isinstance(value, list)



def expect_object(value: Any) -> bool:
    return isinstance(value, dict)


def assert_expected_subset(
    *,
    actual: Any,
    expected: Any,
    failures: list[str],
    path: str,
) -> None:
    if isinstance(expected, dict):
        if not isinstance(actual, dict):
            failures.append(
                f"{path} expected object value, got {type(actual).__name__!s}: {actual!r}"
            )
            return

        for key, expected_value in expected.items():
            if key not in actual:
                failures.append(f"{path}.{key} expected key missing")
                continue
            assert_expected_subset(
                actual=actual[key],
                expected=expected_value,
                failures=failures,
                path=f"{path}.{key}",
            )
        return

    if isinstance(expected, list):
        if not isinstance(actual, list):
            failures.append(
                f"{path} expected list value, got {type(actual).__name__!s}: {actual!r}"
            )
            return
        if len(actual) < len(expected):
            failures.append(
                f"{path} expected list with at least {len(expected)} item(s), got {len(actual)}"
            )
            return

        for index, expected_item in enumerate(expected):
            if index >= len(actual):
                failures.append(f"{path}[{index}] missing expected item")
                return
            assert_expected_subset(
                actual=actual[index],
                expected=expected_item,
                failures=failures,
                path=f"{path}[{index}]",
            )
        return

    if actual != expected:
        failures.append(f"{path} expected {expected!r}, got {actual!r}")



def evaluate_expectations(
    *,
    payload: Any,
    step_status: str | None,
    step_detail: str | None,
    database_uri: str,
    expect: dict[str, Any],
) -> tuple[list[str], str, str]:
    failures: list[str] = []
    error_text, message_text = extract_error_and_message_text(payload, step_detail)
    payload_for_kind_checks = payload
    if (
        isinstance(payload, dict)
        and payload.get("ok") is True
        and "data" in payload
    ):
        payload_for_kind_checks = payload.get("data")

    expected_step_status = expect.get("step_status")
    if isinstance(expected_step_status, str):
        if step_status != expected_step_status:
            failures.append(
                f"step_status mismatch: expected {expected_step_status!r}, got {step_status!r}"
            )

    envelope_kind = expect.get("envelope_kind")
    if isinstance(envelope_kind, str):
        if envelope_kind not in {"list", "object", "null"}:
            failures.append(
                "envelope_kind must be one of 'list', 'object', or 'null'"
            )
        elif envelope_kind == "list" and not expect_list(payload):
            failures.append("envelope_kind=list expected array envelope payload")
        elif envelope_kind == "object" and not expect_object(payload):
            failures.append("envelope_kind=object expected object envelope payload")
        elif envelope_kind == "null" and payload is not None:
            failures.append("envelope_kind=null expected null envelope payload")

    payload_kind = expect.get("payload_kind")
    if isinstance(payload_kind, str) and payload_kind not in {"list", "object", "null"}:
        failures.append("payload_kind must be one of 'list', 'object', or 'null'")

    if payload_kind == "list" and bool(expect.get("data_is_null", False)):
        failures.append("payload_kind=list conflicts with data_is_null=true")
    if payload_kind == "object" and bool(expect.get("data_is_null", False)):
        failures.append("payload_kind=object conflicts with data_is_null=true")
    if payload_kind == "null" and expect.get("data_is_null") is False:
        failures.append("payload_kind=null conflicts with data_is_null=false")

    if payload_kind == "list" and not expect_list(payload_for_kind_checks):
        failures.append("payload_kind=list expected array payload")
    if payload_kind == "object" and not expect_object(payload_for_kind_checks):
        failures.append("payload_kind=object expected object payload")
    if payload_kind == "null" and payload_for_kind_checks is not None:
        failures.append("payload_kind=null expected null payload")

    min_items = expect.get("min_items")
    if isinstance(min_items, int):
        if not isinstance(payload_for_kind_checks, list):
            failures.append("min_items requires list payload")
        elif len(payload_for_kind_checks) < min_items:
            failures.append(
                f"list payload has {len(payload_for_kind_checks)} items, expected at least {min_items}"
            )

    if isinstance(payload, dict):
        code_equals = expect.get("code_equals")
        if isinstance(code_equals, str) and payload.get("code") != code_equals:
            failures.append(
                f"code mismatch: expected {code_equals!r}, got {payload.get('code')!r}"
            )

        reason_equals = expect.get("reason_equals")
        if isinstance(reason_equals, str) and payload.get("reason") != reason_equals:
            failures.append(
                f"reason mismatch: expected {reason_equals!r}, got {payload.get('reason')!r}"
            )

        extension_equals = expect.get("extension_equals")
        if isinstance(extension_equals, str) and payload.get("extension") != extension_equals:
            failures.append(
                "extension mismatch: "
                f"expected {extension_equals!r}, got {payload.get('extension')!r}"
            )

    error_contains_any = expect.get("error_contains_any")
    if isinstance(error_contains_any, list) and error_contains_any:
        haystack = error_text.lower()
        needles = [str(item).lower() for item in error_contains_any if str(item).strip()]
        if needles and not any(needle in haystack for needle in needles):
            failures.append(
                "error text missing expected substring; expected one of "
                f"{error_contains_any}, got: {error_text[:240]!r}"
            )

    message_contains_any = expect.get("message_contains_any")
    if isinstance(message_contains_any, list) and message_contains_any:
        haystack = message_text.lower()
        needles = [str(item).lower() for item in message_contains_any if str(item).strip()]
        if needles and not any(needle in haystack for needle in needles):
            failures.append(
                "message text missing expected substring; expected one of "
                f"{message_contains_any}, got: {message_text[:240]!r}"
            )

    error_not_contains = expect.get("error_not_contains")
    if isinstance(error_not_contains, list) and error_not_contains:
        haystack = error_text.lower()
        for needle in [str(item).lower() for item in error_not_contains if str(item).strip()]:
            if needle in haystack:
                failures.append(f"error text contains forbidden substring: {needle!r}")

    meta_expected = expect.get("meta")
    if isinstance(meta_expected, dict):
        if isinstance(payload, dict):
            assert_expected_subset(
                actual=payload.get("meta"),
                expected=meta_expected,
                failures=failures,
                path="payload.meta",
            )
        else:
            failures.append("meta expectations provided but payload is not an object")

    meta_required = expect.get("meta_required")
    if isinstance(meta_required, list) and meta_required:
        if not isinstance(payload, dict):
            failures.append("meta_required requires an object payload")
        else:
            payload_meta = payload.get("meta")
            if not isinstance(payload_meta, dict):
                failures.append("meta_required expects payload.meta object")
            else:
                for key in meta_required:
                    if not isinstance(key, str):
                        continue
                    if key not in payload_meta:
                        failures.append(f"required meta key missing: {key!r}")

    meta_non_empty = expect.get("meta_non_empty")
    if isinstance(meta_non_empty, list) and meta_non_empty:
        if not isinstance(payload, dict):
            failures.append("meta_non_empty requires an object payload")
        else:
            payload_meta = payload.get("meta")
            if not isinstance(payload_meta, dict):
                failures.append("meta_non_empty expects payload.meta object")
            else:
                for key in meta_non_empty:
                    if not isinstance(key, str):
                        continue
                    value = payload_meta.get(key)
                    if value is None:
                        failures.append(f"required non-empty meta key is null: {key!r}")
                        continue
                    if str(value).strip() == "":
                        failures.append(
                            f"required non-empty meta key has empty string value: {key!r}"
                        )

    data_is_null = expect.get("data_is_null")
    if isinstance(data_is_null, bool):
        if not isinstance(payload, dict):
            failures.append("data_is_null expectation requires an object payload")
        elif data_is_null and payload.get("data") is not None:
            failures.append("payload.data expected null")
        elif not data_is_null and payload.get("data") is None:
            failures.append("payload.data expected non-null")

    if bool(expect.get("redact_database_uri", False)):
        full_text = json.dumps(payload, ensure_ascii=True, sort_keys=True)
        full_text = (full_text + "\n" + error_text).lower()
        for token in redact_forbidden_tokens(database_uri):
            token_l = token.lower()
            if len(token_l) < 4:
                continue
            if token_l in full_text:
                failures.append(f"detected possible secret leakage in output: token={token!r}")

    return failures, error_text, message_text



def rank(severity: str) -> int:
    return SEVERITY_ORDER.get(severity.lower(), -1)



def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run postgres-mcp integration matrix checks."
    )
    parser.add_argument(
        "--matrix",
        default="fixtures/integration_matrix_v1/matrix.json",
        help="Path to matrix fixture JSON (default: fixtures/integration_matrix_v1/matrix.json)",
    )
    parser.add_argument(
        "--probe-bin",
        default=None,
        help="Path to rust mcp-probe binary (default: ../../tools/mcp-probe/rust/target/release/mcp-probe)",
    )
    parser.add_argument(
        "--rust-cmd",
        default=os.getenv("PARITY_RUST_SERVER_CMD"),
        help="Rust server launch command (shell-style string)",
    )
    parser.add_argument(
        "--out-dir",
        default=".tmp/integration_matrix_v1",
        help="Output directory for reports (default: .tmp/integration_matrix_v1)",
    )
    parser.add_argument(
        "--timeout-ms",
        type=int,
        default=12000,
        help="Per-step timeout for probe runs (default: 12000)",
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
        "--fail-on",
        choices=["low", "medium", "high"],
        default="high",
        help="Severity threshold for failing exit code (default: high)",
    )
    parser.add_argument(
        "--with-compose",
        action="store_true",
        help="Start the local docker-compose matrix before checks.",
    )
    parser.add_argument(
        "--compose-file",
        default="fixtures/integration_matrix_v1/docker-compose.yml",
        help="Compose file for --with-compose (default: fixtures/integration_matrix_v1/docker-compose.yml)",
    )
    parser.add_argument(
        "--compose-project",
        default="postgres_mcp_matrix",
        help="Compose project name for --with-compose (default: postgres_mcp_matrix)",
    )
    parser.add_argument(
        "--docker-cmd",
        default=os.getenv("DOCKER_CMD", "docker"),
        help="Docker command prefix (default: DOCKER_CMD env or 'docker'). Example: 'sudo docker'",
    )
    compose_build_policy_default = os.getenv(
        "INTEGRATION_MATRIX_COMPOSE_BUILD_POLICY", "auto"
    ).strip().lower()
    if compose_build_policy_default not in COMPOSE_BUILD_POLICIES:
        compose_build_policy_default = "auto"
    parser.add_argument(
        "--compose-build-policy",
        choices=["auto", "always", "never"],
        default=compose_build_policy_default,
        help=(
            "Compose image build policy for --with-compose "
            "(default: INTEGRATION_MATRIX_COMPOSE_BUILD_POLICY or auto)"
        ),
    )
    parser.add_argument(
        "--print-top-failures",
        type=int,
        default=env_int("INTEGRATION_MATRIX_PRINT_TOP_FAILURES", 0),
        help=(
            "Print up to N high-signal gate failures on error "
            "(default: INTEGRATION_MATRIX_PRINT_TOP_FAILURES or 0)"
        ),
    )
    parser.add_argument(
        "--compose-down",
        dest="compose_down",
        action="store_true",
        help="Bring compose project down after run (default: true with --with-compose)",
    )
    parser.add_argument(
        "--no-compose-down",
        dest="compose_down",
        action="store_false",
        help="Keep compose project running after run.",
    )
    parser.set_defaults(compose_down=True)
    return parser.parse_args()



def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    run_started_at = datetime.now(timezone.utc)
    run_started_monotonic = time.monotonic()

    matrix_path = (root / args.matrix).resolve()
    matrix_doc = load_json(matrix_path)

    probe_bin = args.probe_bin
    if not probe_bin:
        probe_bin = str((root / "../../tools/mcp-probe/rust/target/release/mcp-probe").resolve())
    if not Path(probe_bin).exists():
        print(f"missing probe binary: {probe_bin}", file=sys.stderr)
        return 2

    rust_cmd = args.rust_cmd or str((root / "target/debug/postgres-mcp").resolve())

    targets_raw = matrix_doc.get("targets", [])
    if not isinstance(targets_raw, list) or not targets_raw:
        raise RuntimeError("matrix file must contain non-empty targets array")

    scenarios_raw = matrix_doc.get("scenarios", [])
    if not isinstance(scenarios_raw, list) or not scenarios_raw:
        raise RuntimeError("matrix file must contain non-empty scenarios array")

    compose_started = False
    compose_file = (root / args.compose_file).resolve()
    out_dir = (root / args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    docker_cmd_parts = shlex.split(args.docker_cmd)
    if not docker_cmd_parts:
        raise RuntimeError("docker command prefix resolved to empty value")
    compose_cmd = [
        *docker_cmd_parts,
        "compose",
        "-f",
        str(compose_file),
        "-p",
        args.compose_project,
    ]
    compose_build_used = False
    compose_build_reason = "with_compose_disabled"
    compose_file_sha256 = hash_file_sha256(compose_file) if compose_file.exists() else ""
    compose_inputs_sha256 = ""

    try:
        if args.with_compose:
            apply_compose_default_uris()
            (
                compose_build_used,
                compose_build_reason,
                compose_file_sha256,
                compose_inputs_sha256,
            ) = resolve_compose_build(
                policy=args.compose_build_policy,
                compose_file=compose_file,
                compose_project=args.compose_project,
                docker_cmd=args.docker_cmd,
                out_dir=out_dir,
            )
            compose_up_cmd = compose_cmd + ["up", "-d"]
            if compose_build_used:
                compose_up_cmd.append("--build")
            compose_up_cmd.append("--wait")
            up = run_cmd(compose_up_cmd, cwd=compose_file.parent)
            if up.returncode != 0:
                compose_output = "\n".join(
                    part for part in ((up.stdout or "").strip(), (up.stderr or "").strip()) if part
                )
                if should_allow_compose_unavailable_skip() and is_compose_unavailable_error(
                    compose_output
                ):
                    skip_reason = "docker compose unavailable in this environment"
                    scenario_results = []
                    for scenario in scenarios_raw:
                        if not isinstance(scenario, dict):
                            continue
                        scenario_id = str(scenario.get("id", "")).strip() or "unknown"
                        description = str(scenario.get("description", "")).strip()
                        severity = str(scenario.get("severity", "high")).lower()
                        if severity not in SEVERITY_ORDER:
                            severity = "high"
                        scenario_results.append(
                            {
                                "scenario_id": scenario_id,
                                "description": description,
                                "severity": severity,
                                "status": "skip",
                                "failure_class": None,
                                "target_id": None,
                                "target_label": None,
                                "skip_reason": skip_reason,
                                "compose_error": compose_output,
                            }
                        )

                    summary = {
                        "run_started_at_utc": run_started_at.isoformat(),
                        "run_completed_at_utc": datetime.now(timezone.utc).isoformat(),
                        "duration_sec": round(
                            max(0.0, time.monotonic() - run_started_monotonic), 3
                        ),
                        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                        "matrix": str(matrix_path),
                        "probe_bin": probe_bin,
                        "rust_cmd": rust_cmd,
                        "with_compose": args.with_compose,
                        "compose_file": str(compose_file),
                        "compose_file_sha256": compose_file_sha256,
                        "compose_inputs_sha256": compose_inputs_sha256,
                        "compose_project": args.compose_project,
                        "docker_cmd": args.docker_cmd,
                        "compose_build_policy": args.compose_build_policy,
                        "compose_build_used": compose_build_used,
                        "compose_build_reason": compose_build_reason,
                        "fail_on": args.fail_on,
                        "results_total": len(scenario_results),
                        "results_passed": 0,
                        "results_failed": 0,
                        "results_skipped": len(scenario_results),
                        "failed_by_severity": {"high": 0, "medium": 0, "low": 0},
                        "failed_by_class": {},
                        "gate_failed": False,
                        "gate_failures": 0,
                        "required_target_failures": 0,
                        "compose_unavailable": True,
                        "compose_unavailable_reason": skip_reason,
                        "top_failures": [],
                    }

                    report = {
                        "summary": summary,
                        "targets": [],
                        "results": scenario_results,
                    }
                    report_path = out_dir / "integration_matrix_report.json"
                    with report_path.open("w", encoding="utf-8") as f:
                        json.dump(report, f, indent=2, ensure_ascii=True)
                        f.write("\n")

                    print(
                        "integration matrix skipped: docker compose unavailable; "
                        f"scenarios_marked_skipped={len(scenario_results)}"
                    )
                    print(f"report path: {report_path}")
                    return 0
                print(up.stdout, file=sys.stderr)
                print(up.stderr, file=sys.stderr)
                print("failed to start integration matrix compose", file=sys.stderr)
                return 2
            compose_started = True
            if args.compose_build_policy == "auto":
                save_compose_build_state(
                    compose_build_state_path(out_dir),
                    {
                        "compose_file_sha256": compose_file_sha256,
                        "compose_inputs_sha256": compose_inputs_sha256,
                        "compose_project": args.compose_project,
                        "docker_cmd": args.docker_cmd,
                        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
                    },
                )

        targets = [build_target(item) for item in targets_raw]

        target_inventory: list[dict[str, Any]] = []
        required_target_failures: list[dict[str, Any]] = []

        for target in targets:
            resolved = target.database_uri is not None
            target_inventory.append(
                {
                    "id": target.id,
                    "label": target.label,
                    "postgres_version": target.postgres_version,
                    "extension_state": target.extension_state,
                    "required": target.required,
                    "database_uri_env": target.database_uri_env,
                    "resolved": resolved,
                    "database_uri": redact_database_uri(target.database_uri) if target.database_uri else None,
                    "tags": sorted(target.tags),
                }
            )
            if target.required and not resolved:
                required_target_failures.append(
                    {
                        "scenario_id": "__target_resolution__",
                        "description": "required target database URI is missing",
                        "severity": "high",
                        "target_id": target.id,
                        "target_label": target.label,
                        "status": "fail",
                        "failure_class": "config_error",
                        "failures": [
                            f"required target {target.id} is missing database URI"
                        ],
                    }
                )

        scenario_results: list[dict[str, Any]] = []

        for scenario in scenarios_raw:
            if not isinstance(scenario, dict):
                continue

            scenario_id = str(scenario.get("id", "")).strip() or "unknown"
            description = str(scenario.get("description", "")).strip()
            severity = str(scenario.get("severity", "high")).lower()
            if severity not in SEVERITY_ORDER:
                severity = "high"

            tool_name = scenario.get("tool")
            if not isinstance(tool_name, str) or not tool_name.strip():
                scenario_results.append(
                    {
                        "scenario_id": scenario_id,
                        "description": description,
                        "severity": severity,
                        "status": "fail",
                        "failure_class": "config_error",
                        "target_id": None,
                        "target_label": None,
                        "failures": ["scenario missing non-empty tool field"],
                    }
                )
                continue

            tool_input = scenario.get("input", {})
            if not isinstance(tool_input, dict):
                scenario_results.append(
                    {
                        "scenario_id": scenario_id,
                        "description": description,
                        "severity": severity,
                        "status": "fail",
                        "failure_class": "config_error",
                        "target_id": None,
                        "target_label": None,
                        "failures": ["scenario input must be an object"],
                    }
                )
                continue

            selected_targets = [
                target
                for target in targets
                if target.database_uri and matches_target_selector(scenario, target)
            ]

            if not selected_targets:
                scenario_results.append(
                    {
                        "scenario_id": scenario_id,
                        "description": description,
                        "severity": severity,
                        "status": "fail",
                        "failure_class": "config_error",
                        "target_id": None,
                        "target_label": None,
                        "failures": ["scenario selector resolved no available targets"],
                    }
                )
                continue

            for target in selected_targets:
                database_uri = target.database_uri
                if not database_uri:
                    continue

                mutation = scenario.get("uri_mutation")
                if isinstance(mutation, str) and mutation.strip():
                    database_uri = mutate_database_uri(database_uri, mutation.strip())

                report_prefix = f"{scenario_id}.{target.id}"
                expect = scenario.get("expect", {})
                if not isinstance(expect, dict):
                    expect = {}

                result: dict[str, Any] = {
                    "scenario_id": scenario_id,
                    "description": description,
                    "severity": severity,
                    "target_id": target.id,
                    "target_label": target.label,
                    "target_postgres_version": target.postgres_version,
                    "target_extension_state": target.extension_state,
                    "database_uri": redact_database_uri(database_uri),
                    "uri_mutation": mutation,
                    "tool": tool_name,
                    "input": tool_input,
                }

                try:
                    probe = run_probe(
                        probe_bin=probe_bin,
                        rust_cmd=rust_cmd,
                        tool_name=tool_name,
                        tool_input=tool_input,
                        database_uri=database_uri,
                        out_dir=out_dir,
                        report_prefix=report_prefix,
                        timeout_ms=args.timeout_ms,
                        retries=args.retries,
                        retry_delay_ms=args.retry_delay_ms,
                    )

                    failures, error_text, message_text = evaluate_expectations(
                        payload=probe.payload,
                        step_status=probe.step_status,
                        step_detail=probe.step_detail,
                        database_uri=database_uri,
                        expect=expect,
                    )

                    status = "pass" if not failures else "fail"
                    result.update(
                        {
                            "status": status,
                            "failure_class": classify_failure_messages(failures)
                            if status == "fail"
                            else None,
                            "failures": failures,
                            "probe_exit_code": probe.probe_exit_code,
                            "step_status": probe.step_status,
                            "step_detail": probe.step_detail,
                            "payload": probe.payload,
                            "error_text": error_text,
                            "message_text": message_text,
                        }
                    )
                except Exception as exc:
                    result.update(
                        {
                            "status": "fail",
                            "failures": [f"probe execution failed: {exc}"],
                            "probe_exit_code": None,
                            "step_status": None,
                            "step_detail": None,
                            "failure_class": "infra_error",
                            "payload": None,
                            "error_text": str(exc),
                            "message_text": "",
                        }
                    )

                scenario_results.append(result)

        all_results = required_target_failures + scenario_results

        threshold_rank = rank(args.fail_on)
        fail_gate_results = [
            item
            for item in all_results
            if item.get("status") == "fail"
            and rank(str(item.get("severity", "high"))) >= threshold_rank
        ]
        top_failures = summarize_top_failures(fail_gate_results, args.print_top_failures)

        skipped = [item for item in all_results if item.get("status") == "skip"]
        failed = [item for item in all_results if item.get("status") == "fail"]
        passed = [item for item in all_results if item.get("status") == "pass"]

        failed_by_severity: dict[str, int] = {"high": 0, "medium": 0, "low": 0}
        failed_by_class: dict[str, int] = {}
        for item in failed:
            sev = str(item.get("severity", "high")).lower()
            if sev in failed_by_severity:
                failed_by_severity[sev] += 1
            failure_class = item.get("failure_class")
            if isinstance(failure_class, str) and failure_class.strip():
                key = failure_class.strip()
                failed_by_class[key] = failed_by_class.get(key, 0) + 1

        run_completed_at = datetime.now(timezone.utc)
        duration_sec = round(max(0.0, time.monotonic() - run_started_monotonic), 3)

        summary = {
            "run_started_at_utc": run_started_at.isoformat(),
            "run_completed_at_utc": run_completed_at.isoformat(),
            "duration_sec": duration_sec,
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "matrix": str(matrix_path),
            "probe_bin": probe_bin,
            "rust_cmd": rust_cmd,
            "with_compose": args.with_compose,
            "compose_file": str(compose_file),
            "compose_file_sha256": compose_file_sha256,
            "compose_inputs_sha256": compose_inputs_sha256,
            "compose_project": args.compose_project,
            "docker_cmd": args.docker_cmd,
            "compose_build_policy": args.compose_build_policy,
            "compose_build_used": compose_build_used,
            "compose_build_reason": compose_build_reason,
            "fail_on": args.fail_on,
            "results_total": len(all_results),
            "results_passed": len(passed),
            "results_failed": len(failed),
            "results_skipped": len(skipped),
            "failed_by_severity": failed_by_severity,
            "failed_by_class": failed_by_class,
            "gate_failed": len(fail_gate_results) > 0,
            "gate_failures": len(fail_gate_results),
            "required_target_failures": len(required_target_failures),
            "top_failures": top_failures,
        }

        report = {
            "summary": summary,
            "targets": target_inventory,
            "results": all_results,
        }

        report_path = out_dir / "integration_matrix_report.json"
        with report_path.open("w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=True)
            f.write("\n")

        print(
            "integration matrix report: "
            f"passed={summary['results_passed']} failed={summary['results_failed']} "
            f"skipped={summary['results_skipped']} total={summary['results_total']}"
        )
        print(f"report path: {report_path}")
        if summary["gate_failed"]:
            print(
                "gate failure: "
                f"{summary['gate_failures']} scenario(s) at/above severity {args.fail_on} failed"
            )
            if top_failures:
                print("top gate failures:")
                for index, failure in enumerate(top_failures, start=1):
                    print(
                        f"{index}. scenario={failure['scenario_id']} "
                        f"target={failure['target_id']} "
                        f"severity={failure['severity']} "
                        f"class={failure['failure_class']} "
                        f"message={failure['message']}"
                    )
            return 1

        return 0

    finally:
        if args.with_compose and compose_started and args.compose_down:
            down = run_cmd(compose_cmd + ["down", "-v", "--remove-orphans"], cwd=compose_file.parent)
            if down.returncode != 0:
                sys.stderr.write(down.stdout)
                sys.stderr.write(down.stderr)
                sys.stderr.write("failed to clean up compose project\n")


if __name__ == "__main__":
    raise SystemExit(main())
