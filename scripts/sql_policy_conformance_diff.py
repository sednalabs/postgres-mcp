#!/usr/bin/env python3
"""Run SQL policy differential conformance against kernel vectors.

This harness executes the local Rust conformance binary, writes a deterministic
field-level report, and emits hash-pinned artifact metadata for release signoff.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from pathlib import Path


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def parse_args(root: Path) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Compare postgres runtime SQL policy outcomes with kernel SQL vectors "
            "and emit deterministic report + artifact hashes."
        )
    )
    parser.add_argument(
        "--vectors",
        type=Path,
        default=(root / "../../mcp-policy-kernel/vectors/sql_restricted_policy.json").resolve(),
        help="Path to kernel SQL vectors.",
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=(
            root
            / ".tmp/sql_policy_conformance/sql_policy_conformance_report.json"
        ).resolve(),
        help="Path to conformance report JSON.",
    )
    parser.add_argument(
        "--artifacts",
        type=Path,
        default=(
            root
            / ".tmp/sql_policy_conformance/sql_policy_conformance_artifacts.json"
        ).resolve(),
        help="Path to artifact-hash metadata JSON.",
    )
    return parser.parse_args()


def run_conformance(root: Path, vectors: Path, report: Path) -> subprocess.CompletedProcess[str]:
    cmd = [
        "cargo",
        "run",
        "--quiet",
        "--bin",
        "sql_policy_conformance",
        "--",
        "--vectors",
        str(vectors),
        "--output",
        str(report),
    ]
    return subprocess.run(
        cmd,
        cwd=root,
        check=False,
        text=True,
        capture_output=True,
    )


def write_artifacts(vectors: Path, report: Path, artifacts: Path) -> None:
    artifacts.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "vectors": {
            "path": str(vectors),
            "sha256": sha256_file(vectors),
        },
        "report": {
            "path": str(report),
            "sha256": sha256_file(report),
        },
    }
    artifacts.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    args = parse_args(root)

    if not args.vectors.exists():
        print(f"missing vectors file: {args.vectors}", file=sys.stderr)
        return 2

    args.report.parent.mkdir(parents=True, exist_ok=True)
    completed = run_conformance(root, args.vectors, args.report)

    if completed.stdout:
        print(completed.stdout.strip())
    if completed.stderr:
        print(completed.stderr.strip(), file=sys.stderr)

    if not args.report.exists():
        print(
            "conformance binary did not emit report file: "
            f"{args.report}",
            file=sys.stderr,
        )
        return completed.returncode or 2

    write_artifacts(args.vectors, args.report, args.artifacts)
    print(f"wrote artifacts metadata: {args.artifacts}")

    return completed.returncode


if __name__ == "__main__":
    raise SystemExit(main())
