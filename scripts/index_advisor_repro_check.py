#!/usr/bin/env python3
"""Index-advisor reproducibility and semantic comparison harness.

This harness executes seeded workload cases against both Python and Rust
implementations via mcp-probe stdio runs, normalizes recommendation semantics,
checks Rust run-to-run reproducibility, and validates Rust recommendations
against Python baseline + workload snapshots.
"""

from __future__ import annotations

import argparse
import ast
import json
import os
import re
import shlex
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote, unquote, urlparse

DEFAULT_LOCAL_DB_URI = "postgresql://nbn_dev_user:nbn_dev_user@127.0.0.1:54322/nbn_dev"


def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def parse_command(command: str) -> tuple[str, list[str]]:
    parts = shlex.split(command)
    if not parts:
        raise ValueError("empty server command")
    return parts[0], parts[1:]


def normalize_database_uri(uri: str) -> str:
    if uri.startswith("postgresql+psycopg://"):
        return "postgresql://" + uri[len("postgresql+psycopg://") :]
    return uri


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


def _run_psql_sql(
    host: str,
    port: str,
    user: str,
    database: str,
    password: str,
    sql: str,
    *,
    on_error_stop: bool = True,
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
    cmd.extend(["-c", sql])
    return subprocess.run(cmd, check=False, capture_output=True, text=True, env=env)


def seed_index_advisor_fixture_data(database_uri: str) -> dict[str, Any]:
    parsed = urlparse(database_uri)
    if parsed.scheme not in {"postgres", "postgresql", "postgresql+psycopg"}:
        raise RuntimeError(f"unsupported database URI scheme for seeding: {parsed.scheme}")

    database = parsed.path.lstrip("/") or "postgres"
    host = parsed.hostname or "127.0.0.1"
    port = str(parsed.port or 5432)
    user = parsed.username or "postgres"
    password = unquote(parsed.password or "")

    sql = """
CREATE TABLE IF NOT EXISTS public.advisor_test (
  id integer PRIMARY KEY,
  user_id integer NOT NULL,
  status text NOT NULL,
  created_at timestamptz NOT NULL,
  metadata jsonb NOT NULL
);
TRUNCATE public.advisor_test;
INSERT INTO public.advisor_test (id, user_id, status, created_at, metadata)
SELECT
  g,
  (g % 100),
  CASE WHEN g % 2 = 0 THEN 'active' ELSE 'inactive' END,
  NOW() - (g || ' minutes')::interval,
  jsonb_build_object('tier', CASE WHEN g % 3 = 0 THEN 'pro' ELSE 'basic' END)
FROM generate_series(1, 5000) g;
ANALYZE public.advisor_test;
""".strip()

    completed = _run_psql_sql(host, port, user, database, password, sql)
    if completed.returncode != 0:
        stderr = (completed.stderr or "").strip()
        raise RuntimeError(f"failed to seed index advisor fixtures: {stderr[:500]}")

    return {
        "seeded": True,
        "table": "public.advisor_test",
        "rows": 5000,
    }


def run_probe_script(
    probe_bin: str,
    scenario: dict[str, Any],
    report_out: Path,
    database_uri: str,
    timeout_ms: int,
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


def build_scenario(command: str, tool_input: dict[str, Any]) -> dict[str, Any]:
    cmd, args = parse_command(command)
    return {
        "transport": "stdio",
        "command": cmd,
        "args": args,
        "steps": [
            {
                "id": "advisor_case",
                "tool": "analyze_query_indexes",
                "input": tool_input,
            }
        ],
    }


def get_tool_step(report: dict[str, Any]) -> dict[str, Any]:
    for step in report.get("steps", []):
        name = step.get("name")
        if name == "tool.advisor_case":
            return step
    raise RuntimeError("missing tool.advisor_case step in probe report")


def _extract_actual(step: dict[str, Any]) -> Any:
    data = step.get("data")
    if not isinstance(data, dict):
        return None
    return data.get("actual")


def _normalize_ident(value: str) -> str:
    value = value.strip()
    value = value.replace('"', "")
    value = value.replace("`", "")
    value = value.strip().lower()
    return value


def _parse_index_definition(definition: str) -> tuple[str | None, list[str], str | None]:
    table = None
    using = None
    columns: list[str] = []

    table_match = re.search(r"(?i)\bON\s+([^(\\s]+)", definition)
    if table_match:
        table = _normalize_ident(table_match.group(1))

    using_match = re.search(r"(?i)\bUSING\s+([a-z_][a-z0-9_]*)", definition)
    if using_match:
        using = using_match.group(1).lower()

    cols_match = re.search(r"\(([^()]*)\)\s*$", definition)
    if cols_match:
        raw_cols = cols_match.group(1)
        for piece in raw_cols.split(","):
            col = _normalize_ident(piece)
            if col:
                columns.append(col)

    return table, columns, using


def _signature(table: str, columns: list[str], using: str) -> str:
    return f"{_normalize_ident(table)}|{','.join(_normalize_ident(c) for c in columns)}|{using.lower()}"


def _semantic_signature(signature: str) -> str:
    parts = signature.split("|")
    if len(parts) != 3:
        return signature
    table, cols, using = parts
    col_items = [c for c in cols.split(",") if c]
    col_items.sort()
    return f"{table}|{','.join(col_items)}|{using}"


def _qualified_column_set(signatures: set[str]) -> set[str]:
    out: set[str] = set()
    for signature in signatures:
        parts = signature.split("|")
        if len(parts) != 3:
            continue
        table, cols, _using = parts
        for col in [c for c in cols.split(",") if c]:
            out.add(f"{table}|{col}")
    return out


def extract_python_signatures(actual: Any) -> list[str]:
    if not isinstance(actual, dict):
        return []

    structured = actual.get("structuredContent")
    if not isinstance(structured, dict):
        return []

    result_list = structured.get("result")
    if not isinstance(result_list, list) or not result_list:
        return []

    text_value = None
    first = result_list[0]
    if isinstance(first, dict):
        maybe_text = first.get("text")
        if isinstance(maybe_text, str):
            text_value = maybe_text

    if not text_value:
        return []

    try:
        payload = ast.literal_eval(text_value)
    except (SyntaxError, ValueError):
        return []

    if not isinstance(payload, dict):
        return []
    recommendations = payload.get("recommendations")
    if not isinstance(recommendations, list):
        return []

    signatures: set[str] = set()
    for rec in recommendations:
        if not isinstance(rec, dict):
            continue

        table = rec.get("index_target_table")
        columns = rec.get("index_target_columns")
        using = None

        if isinstance(columns, tuple):
            columns = list(columns)
        if not isinstance(columns, list):
            columns = []
        columns_norm = [_normalize_ident(str(c)) for c in columns if str(c).strip()]

        index_definition = rec.get("index_definition")
        if isinstance(index_definition, str) and index_definition.strip():
            parsed_table, parsed_cols, parsed_using = _parse_index_definition(index_definition)
            if not table and parsed_table:
                table = parsed_table
            if not columns_norm and parsed_cols:
                columns_norm = parsed_cols
            if parsed_using:
                using = parsed_using

        if not isinstance(table, str) or not table.strip() or not columns_norm:
            continue
        if using is None:
            using = "btree"
        signatures.add(_signature(table, columns_norm, using))

    return sorted(signatures)


def extract_rust_signatures(actual: Any) -> list[str]:
    if not isinstance(actual, dict):
        return []

    structured = actual.get("structuredContent")
    if not isinstance(structured, dict):
        return []

    rec_container = structured.get("recommendations")
    if not isinstance(rec_container, dict):
        return []

    recommendations = rec_container.get("recommendations")
    if not isinstance(recommendations, list):
        return []

    signatures: set[str] = set()
    for rec in recommendations:
        if not isinstance(rec, dict):
            continue
        table = rec.get("table")
        columns = rec.get("columns")
        using = rec.get("using")
        if not isinstance(table, str) or not table.strip():
            continue
        if not isinstance(columns, list) or not columns:
            continue
        if not isinstance(using, str) or not using.strip():
            using = "btree"
        col_norm = [_normalize_ident(str(c)) for c in columns if str(c).strip()]
        if not col_norm:
            continue
        signatures.add(_signature(table, col_norm, using))

    return sorted(signatures)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run index-advisor reproducibility + Python baseline comparison harness."
    )
    parser.add_argument(
        "--fixtures",
        default="fixtures/index_advisor_v1/workloads.json",
        help="Path to workload fixtures JSON (default: fixtures/index_advisor_v1/workloads.json)",
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
        "--runs",
        type=int,
        default=3,
        help="Number of Rust repeat runs per workload (default: 3)",
    )
    parser.add_argument(
        "--timeout-ms",
        type=int,
        default=20000,
        help="Per-step timeout for probe runs (default: 20000)",
    )
    parser.add_argument(
        "--out-dir",
        default=".tmp/index_advisor_v1",
        help="Output directory for reports (default: .tmp/index_advisor_v1)",
    )
    parser.add_argument(
        "--no-seed",
        action="store_true",
        help="Skip fixture seeding SQL.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    fixtures_path = (root / args.fixtures).resolve()
    fixtures_doc = load_json(fixtures_path)
    workloads = fixtures_doc.get("workloads", [])
    if not isinstance(workloads, list) or not workloads:
        raise RuntimeError("workload fixtures file must contain a non-empty workloads array")

    probe_bin = args.probe_bin
    if not probe_bin:
        probe_bin = str((root / "../../tools/mcp-probe/rust/target/release/mcp-probe").resolve())
    if not Path(probe_bin).exists():
        raise RuntimeError(f"missing probe binary: {probe_bin}")

    python_cmd = args.python_cmd
    if not python_cmd:
        default_venv_cmd = Path("/tmp/postgres-mcp-parity-venv/bin/postgres-mcp")
        if default_venv_cmd.exists():
            python_cmd = str(default_venv_cmd)
        else:
            python_cmd = "postgres-mcp"

    rust_cmd = args.rust_cmd or str((root / "target/debug/postgres-mcp").resolve())
    database_uri = resolve_database_uri(args.database_uri)

    seed_info: dict[str, Any] = {"skipped": True}
    if not args.no_seed:
        seed_info = seed_index_advisor_fixture_data(database_uri)

    out_dir = (root / args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    case_results: list[dict[str, Any]] = []
    failed = 0

    for workload in workloads:
        if not isinstance(workload, dict):
            continue
        workload_id = str(workload.get("id", "unknown"))
        queries = workload.get("queries", [])
        if not isinstance(queries, list) or not queries:
            case_results.append(
                {
                    "id": workload_id,
                    "status": "fail",
                    "failures": ["workload queries must be a non-empty array"],
                }
            )
            failed += 1
            continue

        input_payload: dict[str, Any] = {
            "queries": queries,
            "method": workload.get("method", "dta"),
        }
        if "max_index_size_mb" in workload:
            input_payload["max_index_size_mb"] = workload["max_index_size_mb"]

        py_scenario = build_scenario(python_cmd, input_payload)
        py_report_path = out_dir / f"{workload_id}.python.report.json"
        py_report, py_proc = run_probe_script(
            probe_bin,
            py_scenario,
            py_report_path,
            database_uri,
            args.timeout_ms,
        )
        py_step = get_tool_step(py_report)
        py_actual = _extract_actual(py_step)
        py_signatures = extract_python_signatures(py_actual)

        rust_runs: list[list[str]] = []
        rust_run_meta: list[dict[str, Any]] = []
        for idx in range(args.runs):
            rs_scenario = build_scenario(rust_cmd, input_payload)
            rs_report_path = out_dir / f"{workload_id}.rust.run{idx + 1}.report.json"
            rs_report, rs_proc = run_probe_script(
                probe_bin,
                rs_scenario,
                rs_report_path,
                database_uri,
                args.timeout_ms,
            )
            rs_step = get_tool_step(rs_report)
            rs_actual = _extract_actual(rs_step)
            rs_signatures = extract_rust_signatures(rs_actual)
            rust_runs.append(rs_signatures)
            rust_run_meta.append(
                {
                    "run": idx + 1,
                    "probe_exit_code": rs_proc.returncode,
                    "step_status": rs_step.get("status"),
                    "step_detail": rs_step.get("detail"),
                }
            )

        rust_stable = all(run == rust_runs[0] for run in rust_runs[1:]) if rust_runs else True
        rust_signatures = rust_runs[0] if rust_runs else []

        py_set = set(py_signatures)
        rs_set = set(rust_signatures)
        py_semantic_set = {_semantic_signature(sig) for sig in py_set}
        rs_semantic_set = {_semantic_signature(sig) for sig in rs_set}
        py_col_set = _qualified_column_set(py_semantic_set)
        rs_col_set = _qualified_column_set(rs_semantic_set)
        overlap_ratio = (
            1.0
            if not py_semantic_set
            else len(py_semantic_set & rs_semantic_set) / len(py_semantic_set)
        )
        column_overlap_ratio = (
            1.0 if not py_col_set else len(py_col_set & rs_col_set) / len(py_col_set)
        )

        failures: list[str] = []
        if py_proc.returncode != 0:
            failures.append(f"python probe exit code {py_proc.returncode}")
        if not rust_stable:
            failures.append("rust recommendations are not reproducible across runs")

        comparison = workload.get("comparison", {})
        if not isinstance(comparison, dict):
            comparison = {}
        min_overlap_ratio = float(comparison.get("min_python_overlap_ratio", 1.0))
        if overlap_ratio < min_overlap_ratio:
            failures.append(
                f"python/rust recommendation overlap ratio {overlap_ratio:.3f} < required {min_overlap_ratio:.3f}"
            )
        min_column_overlap_ratio = comparison.get("min_python_column_overlap_ratio")
        if min_column_overlap_ratio is not None:
            min_column_overlap_ratio = float(min_column_overlap_ratio)
            if column_overlap_ratio < min_column_overlap_ratio:
                failures.append(
                    "python/rust recommendation column overlap ratio "
                    f"{column_overlap_ratio:.3f} < required {min_column_overlap_ratio:.3f}"
                )

        max_rust_recommendations = comparison.get("max_rust_recommendations")
        if isinstance(max_rust_recommendations, int) and len(rust_signatures) > max_rust_recommendations:
            failures.append(
                f"rust recommendations count {len(rust_signatures)} exceeds max {max_rust_recommendations}"
            )

        rust_snapshot = workload.get("rust_snapshot", {})
        if not isinstance(rust_snapshot, dict):
            rust_snapshot = {}
        required_signatures = rust_snapshot.get("required_signatures", [])
        if not isinstance(required_signatures, list):
            required_signatures = []
        missing_required = [sig for sig in required_signatures if sig not in rs_set]
        if missing_required:
            failures.append(f"missing required rust snapshot signatures: {missing_required}")

        allow_extra = bool(rust_snapshot.get("allow_extra", True))
        if not allow_extra:
            extras = sorted(sig for sig in rs_set if sig not in set(required_signatures))
            if extras:
                failures.append(f"unexpected extra rust snapshot signatures: {extras}")

        status = "pass" if not failures else "fail"
        if status == "fail":
            failed += 1

        case_results.append(
            {
                "id": workload_id,
                "title": workload.get("title"),
                "status": status,
                "failures": failures,
                "python": {
                    "probe_exit_code": py_proc.returncode,
                    "step_status": py_step.get("status"),
                    "step_detail": py_step.get("detail"),
                    "signatures": py_signatures,
                },
                "rust": {
                    "stable": rust_stable,
                    "runs": rust_run_meta,
                    "signatures": rust_signatures,
                },
                "comparison": {
                    "overlap_ratio": overlap_ratio,
                    "column_overlap_ratio": column_overlap_ratio,
                    "missing_from_rust": sorted(py_semantic_set - rs_semantic_set),
                    "extra_in_rust": sorted(rs_semantic_set - py_semantic_set),
                    "min_python_overlap_ratio": min_overlap_ratio,
                    "min_python_column_overlap_ratio": min_column_overlap_ratio,
                },
                "snapshot": {
                    "required_signatures": required_signatures,
                    "allow_extra": allow_extra,
                },
            }
        )

    summary = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "fixtures": str(fixtures_path),
        "probe_bin": probe_bin,
        "python_cmd": python_cmd,
        "rust_cmd": rust_cmd,
        "database_uri": redact_database_uri(database_uri),
        "seed": seed_info,
        "workloads_total": len(case_results),
        "workloads_failed": failed,
        "workloads_passed": len(case_results) - failed,
    }
    report = {"summary": summary, "workloads": case_results}

    report_path = out_dir / "index_advisor_repro_report.json"
    with report_path.open("w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=True)
        f.write("\n")

    print(
        "index advisor repro report: "
        f"passed={summary['workloads_passed']} failed={summary['workloads_failed']} total={summary['workloads_total']}"
    )
    print(f"report path: {report_path}")

    if failed:
        print("failed workloads:")
        for item in case_results:
            if item.get("status") != "fail":
                continue
            reason = "; ".join(item.get("failures", [])[:3])
            print(f"- {item.get('id')}: {reason}")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
