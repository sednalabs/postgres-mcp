#!/usr/bin/env python3
"""Run runtime safety conformance checks for postgres-mcp."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def parse_args(root: Path) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run runtime safety probe checks (policy + execution envelope) and emit "
            "hash-pinned report metadata."
        )
    )
    parser.add_argument(
        "--database-uri",
        type=str,
        help="Optional PostgreSQL URI for online runtime checks (falls back to DATABASE_URI env var).",
    )
    parser.add_argument(
        "--require-db-runtime",
        action="store_true",
        help="Fail when online runtime checks are skipped due to missing DATABASE_URI.",
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=(root / ".tmp/runtime_safety/runtime_safety_probe_report.json").resolve(),
        help="Path to runtime safety report JSON.",
    )
    parser.add_argument(
        "--artifacts",
        type=Path,
        default=(root / ".tmp/runtime_safety/runtime_safety_artifacts.json").resolve(),
        help="Path to artifact metadata JSON.",
    )
    return parser.parse_args()


def run_probe(
    root: Path,
    *,
    report: Path,
    database_uri: str | None,
    require_db_runtime: bool,
) -> subprocess.CompletedProcess[str]:
    cmd = [
        "cargo",
        "run",
        "--quiet",
        "--bin",
        "runtime_safety_probe",
        "--",
        "--output",
        str(report),
    ]
    if database_uri:
        cmd.extend(["--database-uri", database_uri])
    if require_db_runtime:
        cmd.append("--require-db-runtime")
    return subprocess.run(cmd, cwd=root, text=True, capture_output=True, check=False)


def write_artifacts(
    *,
    report: Path,
    artifacts: Path,
    database_uri_source: str,
    require_db_runtime: bool,
    report_json: dict,
) -> None:
    artifacts.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "report": {
            "path": str(report),
            "sha256": sha256_file(report),
            "pass": bool(report_json.get("pass", False)),
            "failed_checks": int(report_json.get("failed_checks", 0)),
        },
        "runtime": {
            "database_uri_source": database_uri_source,
            "require_db_runtime": require_db_runtime,
        },
    }
    artifacts.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    args = parse_args(root)

    env_database_uri = os.getenv("DATABASE_URI")
    database_uri = args.database_uri or env_database_uri
    if args.database_uri:
        database_uri_source = "arg"
    elif env_database_uri:
        database_uri_source = "env"
    else:
        database_uri_source = "none"

    args.report.parent.mkdir(parents=True, exist_ok=True)
    completed = run_probe(
        root,
        report=args.report,
        database_uri=database_uri,
        require_db_runtime=args.require_db_runtime,
    )

    if completed.stdout:
        print(completed.stdout.strip())
    if completed.stderr:
        print(completed.stderr.strip(), file=sys.stderr)

    if not args.report.exists():
        print(f"runtime safety probe did not emit report file: {args.report}", file=sys.stderr)
        return completed.returncode or 2

    report_json = json.loads(args.report.read_text(encoding="utf-8"))
    write_artifacts(
        report=args.report,
        artifacts=args.artifacts,
        database_uri_source=database_uri_source,
        require_db_runtime=args.require_db_runtime,
        report_json=report_json,
    )
    print(f"wrote artifacts metadata: {args.artifacts}")

    if not bool(report_json.get("pass", False)):
        return completed.returncode or 1
    return completed.returncode


if __name__ == "__main__":
    raise SystemExit(main())
