#!/usr/bin/env python3
"""Run a lightweight post-merge ergonomics validation pass.

This command runs:

1) the existing index advisor repro pack (index_advisor_repro_check.sh), then
2) operator-oriented `execute_sql` scenarios from the fixture file and computes
   error-loop and correction-latency metrics.
"""

from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
import tempfile
import time
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def parse_command(command: str) -> tuple[str, list[str]]:
    parts = shlex.split(command)
    if not parts:
        raise ValueError("empty command string")
    return parts[0], parts[1:]


def resolve_database_uri(cli_uri: str | None = None) -> str:
    if cli_uri:
        return cli_uri
    env_uri = os.getenv("DATABASE_URI")
    if env_uri:
        return env_uri
    env_url = os.getenv("DATABASE_URL")
    if env_url:
        return env_url
    return "postgresql://nbn_dev_user:nbn_dev_user@127.0.0.1:54322/nbn_dev"


def load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def run_command(command: list[str], *, cwd: Path, env: dict[str, str], check: bool = False) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        check=check,
        cwd=str(cwd),
        env=env,
        text=True,
        capture_output=True,
    )


def normalize_server_command(server_cmd: str, root: Path) -> str:
    cmd, args = parse_command(server_cmd)
    cmd_path = Path(cmd)

    if not cmd_path.is_absolute():
        absolute_candidates = [
            root / cmd,
            root / "target" / "debug" / cmd_path.name,
            root / "target" / "release" / cmd_path.name,
        ]
        for candidate in absolute_candidates:
            if candidate.exists():
                cmd = str(candidate)
                break
    return " ".join([shlex.quote(cmd)] + [shlex.quote(arg) for arg in args])


def extract_payload(actual: Any) -> Any:
    if not isinstance(actual, dict):
        return actual

    structured = actual.get("structuredContent")
    if structured is not None:
        return structured

    content = actual.get("content")
    if isinstance(content, list):
        for block in content:
            if not isinstance(block, dict):
                continue
            if block.get("type") == "text" and isinstance(block.get("text"), str):
                raw = block["text"]
                try:
                    return json.loads(raw)
                except json.JSONDecodeError:
                    return raw
    return actual


def resolve_json_pointer(value: Any, pointer: str) -> tuple[bool, Any]:
    if pointer == "":
        return True, value
    if not pointer.startswith("/"):
        raise ValueError(f"json pointer must start with '/': {pointer}")

    current = value
    for token in pointer.lstrip("/").split("/"):
        token = token.replace("~1", "/").replace("~0", "~")
        if isinstance(current, dict):
            if token not in current:
                return False, None
            current = current[token]
            continue
        if isinstance(current, list):
            try:
                index = int(token)
            except ValueError:
                return False, None
            if index < 0 or index >= len(current):
                return False, None
            current = current[index]
            continue
        return False, None

    return True, current


def build_attempt_input(attempt: dict[str, Any]) -> dict[str, Any]:
    explicit_input = attempt.get("input")
    if explicit_input is not None:
        if not isinstance(explicit_input, dict):
            raise ValueError("attempt.input must be an object")
        return explicit_input

    sql = attempt.get("sql")
    if sql is None:
        raise ValueError("attempt must provide either sql or input")
    return {"sql": str(sql)}


def extract_error_view(payload: Any) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    nested_error = payload.get("error")
    if isinstance(nested_error, dict):
        return nested_error
    return payload


def parse_error_payload(payload: Any) -> tuple[str | None, str | None]:
    error_view = extract_error_view(payload)
    if error_view is None:
        return None, None

    error_code = error_view.get("code")
    if isinstance(error_code, str) and error_code.strip():
        return error_code.strip(), None

    message = error_view.get("message")
    if isinstance(message, str) and message.strip():
        return None, message

    error = error_view.get("error")
    if isinstance(error, str) and error.strip():
        return None, error

    return None, None


def normalise_error_message(payload: Any, default: str, fallback_detail: str | None = None) -> str:
    if isinstance(payload, str):
        if payload.strip():
            return payload
    error_view = extract_error_view(payload)
    if error_view is not None:
        message = error_view.get("message")
        if isinstance(message, str) and message.strip():
            return message
        error = error_view.get("error")
        if isinstance(error, str) and error.strip():
            return error

    if isinstance(fallback_detail, str) and fallback_detail.strip():
        return fallback_detail
    return default


def check_attempt_expectations(
    attempt: dict[str, Any],
    payload: Any,
    error_code: str | None,
    error_message: str | None,
) -> str | None:
    expected_code = attempt.get("expect_error_code")
    if expected_code is not None and error_code != str(expected_code):
        return f"expected error_code={expected_code!r}, got {error_code!r}"

    expected_error_contains = attempt.get("expect_error_contains")
    if expected_error_contains is not None:
        expected_substring = str(expected_error_contains)
        if not error_message or expected_substring not in error_message:
            return (
                f"expected error message to contain {expected_substring!r}, "
                f"got {error_message!r}"
            )

    pointer_expectations = attempt.get("expect_json_pointers")
    if pointer_expectations is not None:
        if not isinstance(pointer_expectations, dict):
            return "expect_json_pointers must be an object"
        for pointer, expected_value in pointer_expectations.items():
            found, actual_value = resolve_json_pointer(payload, str(pointer))
            if not found:
                return f"expected payload pointer {pointer!r} to exist"
            if actual_value != expected_value:
                return (
                    f"expected payload pointer {pointer!r} to equal "
                    f"{expected_value!r}, got {actual_value!r}"
                )

    return None


def find_tool_step(report: dict[str, Any], attempt_id: str) -> dict[str, Any] | None:
    exact_attempt = f"tool.{attempt_id}"
    for step in report.get("steps", []):
        if step.get("name") == exact_attempt:
            return step

    for step in report.get("steps", []):
        name = step.get("name")
        if isinstance(name, str) and name.startswith("tool.execute_sql"):
            return step

    for step in report.get("steps", []):
        name = step.get("name")
        if isinstance(name, str) and name.startswith("tool."):
            return step

    return None


def find_step_detail_by_name(report: dict[str, Any], step_name: str) -> str | None:
    for step in report.get("steps", []):
        if step.get("name") == step_name:
            return step.get("detail")
    return None


@dataclass
class AttemptResult:
    label: str
    expect_success: bool
    status: str
    success: bool
    exit_code: int
    error_code: str | None
    error_message: str | None
    elapsed_ms: float


def run_execute_sql_attempt(
    probe_bin: str,
    server_cmd: str,
    database_uri: str,
    attempt: dict[str, Any],
    out_dir: Path,
) -> AttemptResult:
    cmd, args = parse_command(server_cmd)
    attempt_id = f"attempt_{int(time.time() * 1000000)}"
    tool_input = build_attempt_input(attempt)

    scenario = {
        "transport": "stdio",
        "command": cmd,
        "args": args,
        "steps": [
            {"id": "execute_sql", "tool": "execute_sql", "input": tool_input}
        ],
    }

    with tempfile.NamedTemporaryFile("w", suffix=".json", encoding="utf-8", delete=False) as temp:
        json.dump(scenario, temp, indent=2)
        scenario_path = Path(temp.name)

    report_path = out_dir / f"{attempt_id}.probe.json"
    started_at = time.perf_counter()
    completed = run_command(
        [
            probe_bin,
            "run",
            "--script",
            str(scenario_path),
            "--out",
            str(report_path),
            "--json",
            "--timeout-ms",
            "30000",
        ],
        cwd=out_dir,
        env={
            **os.environ,
            "MCP_PROBE_ALLOW_STDIO": "1",
            "DATABASE_URI": database_uri,
            "POSTGRES_MCP_STARTUP_DB_CONNECT": os.getenv(
                "POSTGRES_MCP_STARTUP_DB_CONNECT",
                "warn",
            ),
            "POSTGRES_MCP_STARTUP_DB_CONNECT_TIMEOUT_SEC": os.getenv(
                "POSTGRES_MCP_STARTUP_DB_CONNECT_TIMEOUT_SEC",
                "3",
            ),
        },
    )
    elapsed_ms = (time.perf_counter() - started_at) * 1000

    try:
        scenario_path.unlink(missing_ok=True)
    except OSError:
        pass

    try:
        scenario_report = load_json(report_path)
    except FileNotFoundError as exc:
        status = "missing_report"
        error_code = None
        error_message = f"probe did not emit report: {exc}"
        return AttemptResult("", True, status, False, completed.returncode, error_code, error_message, elapsed_ms)

    step = find_tool_step(scenario_report, attempt_id)
    if step is None:
        connect_detail = find_step_detail_by_name(scenario_report, "connect")
        if connect_detail:
            status = "connect_error"
            error_message = f"probe connect step failed: {connect_detail}"
        else:
            status = "missing_tool_step"
            step_names = [
                str(step.get("name"))
                for step in scenario_report.get("steps", [])
                if isinstance(step.get("name"), str)
            ]
            error_message = (
                "probe report did not include execute_sql step; steps: "
                + ", ".join(step_names)
            )
        return AttemptResult("", True, status, False, completed.returncode, None, error_message, elapsed_ms)

    payload = None
    step_status = step.get("status")
    step_detail = step.get("detail")
    try:
        data = step.get("data", {})
        actual = data.get("actual", {})
        payload = extract_payload(actual)
    except Exception:  # pragma: no cover - defensive for malformed reports
        payload = None

    if isinstance(payload, dict) and ("error" in payload or "code" in payload or "message" in payload or "details" in payload):
        error_code, parsed_message = parse_error_payload(payload)
        if parsed_message is None:
            parsed_message = normalise_error_message(payload, "tool returned error payload", step_detail if isinstance(step_detail, str) else None)
        if error_code is None and isinstance(payload.get("message"), str):
            # Common DB errors often include SQLSTATE in message text.
            match = re.search(r"SQLSTATE\s+([A-Za-z0-9]+)", payload.get("message", ""))
            if match:
                error_code = match.group(1)
        status = "error"
        result = AttemptResult(
            "",
            False,
            status,
            False,
            completed.returncode,
            str(error_code) if error_code is not None else None,
            parsed_message,
            elapsed_ms,
        )
        expectation_error = check_attempt_expectations(
            attempt,
            payload,
            result.error_code,
            result.error_message,
        )
        if expectation_error is not None:
            result.status = "assertion_failed"
            result.error_message = expectation_error
        return result

    if step_status == "error":
        status = "error"
        result = AttemptResult(
            "",
            False,
            status,
            False,
            completed.returncode,
            None,
            normalise_error_message(payload, "tool step reported error", step_detail if isinstance(step_detail, str) else None),
            elapsed_ms,
        )
        expectation_error = check_attempt_expectations(
            attempt,
            payload,
            result.error_code,
            result.error_message,
        )
        if expectation_error is not None:
            result.status = "assertion_failed"
            result.error_message = expectation_error
        return result

    if not isinstance(payload, list):
        if not isinstance(payload, dict):
            status = "unexpected_payload"
            error_message = f"unexpected payload shape: {type(payload).__name__}"
            return AttemptResult("", True, status, False, completed.returncode, None, error_message, elapsed_ms)

    status = "success"
    result = AttemptResult("", True, status, True, completed.returncode, None, None, elapsed_ms)
    expectation_error = check_attempt_expectations(attempt, payload, None, None)
    if expectation_error is not None:
        result.status = "assertion_failed"
        result.success = False
        result.error_message = expectation_error
    return result


def evaluate_scenario(
    probe_bin: str,
    server_cmd: str,
    database_uri: str,
    scenario: dict[str, Any],
    out_dir: Path,
) -> dict[str, Any]:
    scenario_id = scenario.get("id", "unknown")
    scenario_name = scenario.get("name", scenario_id)
    attempts = scenario.get("attempts", [])

    attempt_results: list[AttemptResult] = []
    start_ms = time.perf_counter() * 1000
    passed = False
    first_success_ms: float | None = None

    for index, attempt in enumerate(attempts, start=1):
        label = str(attempt.get("label", f"attempt-{index}"))
        expect_success = bool(attempt.get("expect_success", index == len(attempts)))
        result = run_execute_sql_attempt(
            probe_bin=probe_bin,
            server_cmd=server_cmd,
            database_uri=database_uri,
            attempt=attempt,
            out_dir=out_dir,
        )
        result.label = label
        result.expect_success = expect_success
        attempt_results.append(result)

        elapsed_from_start_ms = (time.perf_counter() * 1000) - start_ms
        if result.success and first_success_ms is None:
            first_success_ms = elapsed_from_start_ms
        if result.success and expect_success:
            passed = True
            break
        if expect_success and not result.success:
            # If user expected this attempt to succeed but it did not, stop early.
            break

    failure_count = len([item for item in attempt_results if not item.success])
    corrected = passed and attempt_results and attempt_results[-1].success
    correction_latency_ms = first_success_ms if first_success_ms is not None else None

    return {
        "id": scenario_id,
        "name": scenario_name,
        "attempts": [
            {
                "label": item.label,
                "expect_success": item.expect_success,
                "status": item.status,
                "success": item.success,
                "exit_code": item.exit_code,
                "error_code": item.error_code,
                "error_message": item.error_message,
                "elapsed_ms": round(item.elapsed_ms, 2),
            }
            for item in attempt_results
        ],
        "summary": {
            "attempts": len(attempt_results),
            "error_loop_count": failure_count,
            "correction_latency_ms": correction_latency_ms and round(correction_latency_ms, 2),
            "passed": bool(corrected),
            "final_success": bool(attempt_results and attempt_results[-1].success),
        },
    }


def run_repro_pack(root: Path, out_dir: Path, database_uri: str) -> dict[str, Any]:
    script_path = root / "scripts" / "index_advisor_repro_check.sh"
    report_path = out_dir / "index_advisor_repro_report.json"

    command = [
        str(script_path),
        "--out-dir",
        str(out_dir),
    ]
    if database_uri:
        command.extend(["--database-uri", database_uri])

    completed = run_command(command, cwd=root, env={**os.environ, "DATABASE_URI": database_uri})
    report: dict[str, Any]
    if report_path.exists():
        report = load_json(report_path)
    else:
        report = {
            "error": "repro report missing",
            "exit_code": completed.returncode,
        }

    return {
        "exit_code": completed.returncode,
        "report_path": str(report_path),
        "report": report,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run post-merge ergonomics rollout validation and emit a JSON report."
    )
    parser.add_argument(
        "--fixtures",
        default="fixtures/ergonomics_validation/analyst_scenarios.json",
        help="Path to validation scenario fixture JSON.",
    )
    parser.add_argument(
        "--out-dir",
        default=".tmp/ergonomics_validation",
        help="Output directory for validation reports.",
    )
    parser.add_argument(
        "--database-uri",
        default=None,
        help="Database URI override.",
    )
    parser.add_argument(
        "--probe-bin",
        default="",
        help="Path to mcp-probe binary.",
    )
    parser.add_argument(
        "--server-cmd",
        default="",
        help="Rust server command used by mcp-probe for execute_sql attempts.",
    )
    parser.add_argument(
        "--skip-repro-pack",
        action="store_true",
        help="Skip index advisor repro pack execution.",
    )
    args = parser.parse_args()

    root = Path(__file__).resolve().parent.parent
    out_dir = (root / args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    probe_bin = args.probe_bin
    if not probe_bin:
        probe_bin = str((root / "../../tools/mcp-probe/rust/target/release/mcp-probe").resolve())
    if not Path(probe_bin).exists():
        raise RuntimeError(f"missing probe binary: {probe_bin}")

    database_uri = resolve_database_uri(args.database_uri)
    server_cmd = args.server_cmd.strip()
    if not server_cmd:
        server_cmd = f"{root / 'target/debug/postgres-mcp'} --access-mode=unrestricted --startup-db-connect=warn"
    server_cmd = normalize_server_command(server_cmd, root)
    fixtures_path = (root / args.fixtures).resolve()
    scenario_doc = load_json(fixtures_path)
    scenarios = scenario_doc.get("scenarios", [])

    report: dict[str, Any] = {
        "schema_version": "1",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "scenario_id": scenario_doc.get("id", "unknown"),
        "database_uri": database_uri,
        "probe_bin": probe_bin,
        "server_cmd": server_cmd,
        "repro_pack": None,
        "scenarios": [],
    }

    if not args.skip_repro_pack:
        report["repro_pack"] = run_repro_pack(root, out_dir, database_uri)

    for scenario in scenarios:
        report["scenarios"].append(
            evaluate_scenario(
                probe_bin=probe_bin,
                server_cmd=server_cmd,
                database_uri=database_uri,
                scenario=scenario,
                out_dir=out_dir,
            )
        )

    report_path = out_dir / "rollout_validation_report.json"
    with report_path.open("w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=True)
        f.write("\n")

    passed_scenarios = [
        item for item in report["scenarios"] if item.get("summary", {}).get("passed")
    ]
    print(
        f"validation report path: {report_path}\n"
        f"workflows passed: {len(passed_scenarios)}/{len(report['scenarios'])}\n"
        f"repro pack exit: {None if report['repro_pack'] is None else report['repro_pack'].get('exit_code')}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
