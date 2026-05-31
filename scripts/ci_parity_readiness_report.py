#!/usr/bin/env python3
"""Build a CI parity readiness report from integration matrix outputs."""

from __future__ import annotations

import argparse
import json
import statistics
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate readiness report for advisory-to-required CI parity policy."
    )
    parser.add_argument(
        "--report",
        action="append",
        default=[],
        help=(
            "Integration matrix report path (repeatable). "
            "Default: .tmp/integration_matrix_v1/integration_matrix_report.json"
        ),
    )
    parser.add_argument(
        "--out-json",
        default=".tmp/integration_matrix_v1/ci_parity_readiness_report.json",
        help="Output JSON report path.",
    )
    parser.add_argument(
        "--out-md",
        default=".tmp/integration_matrix_v1/ci_parity_readiness_report.md",
        help="Output markdown summary path.",
    )
    parser.add_argument(
        "--min-runs",
        type=int,
        default=10,
        help="Minimum analyzed runs required before readiness can pass.",
    )
    parser.add_argument(
        "--max-gate-failure-rate-pct",
        type=float,
        default=5.0,
        help="Maximum allowed gate-failure rate percentage.",
    )
    parser.add_argument(
        "--max-infra-failure-rate-pct",
        type=float,
        default=10.0,
        help="Maximum allowed infra-error run rate percentage.",
    )
    parser.add_argument(
        "--max-median-duration-sec",
        type=float,
        default=900.0,
        help="Maximum allowed median run duration in seconds.",
    )
    parser.add_argument(
        "--min-compose-reuse-rate-pct",
        type=float,
        default=0.0,
        help=(
            "Minimum allowed compose auto-reuse rate percentage "
            "for runs using compose_build_policy=auto."
        ),
    )
    parser.add_argument(
        "--fail-on-unready",
        action="store_true",
        help="Exit non-zero when readiness does not pass.",
    )
    return parser.parse_args()


def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def collect_report_paths(raw_paths: list[str]) -> list[Path]:
    if not raw_paths:
        raw_paths = [".tmp/integration_matrix_v1/integration_matrix_report.json"]

    seen: set[str] = set()
    paths: list[Path] = []
    for raw in raw_paths:
        candidate = Path(raw)
        normalized = str(candidate.resolve() if candidate.exists() else candidate)
        if normalized in seen:
            continue
        seen.add(normalized)
        paths.append(candidate)
    return paths


def pct(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 3)


def build_markdown(report: dict[str, Any]) -> str:
    metrics = report["metrics"]
    thresholds = report["thresholds"]
    checks = report["checks"]
    failure_classes = report["failure_classes"]

    lines: list[str] = []
    lines.append("## CI Parity Readiness")
    lines.append("")
    lines.append(f"- Ready: `{str(report['ready']).lower()}`")
    lines.append(f"- Runs analyzed: `{metrics['runs_total']}`")
    lines.append(
        f"- Gate failure rate: `{metrics['gate_failure_rate_pct']}%` "
        f"(target <= `{thresholds['max_gate_failure_rate_pct']}%`)"
    )
    lines.append(
        f"- Infra failure rate: `{metrics['infra_failure_rate_pct']}%` "
        f"(target <= `{thresholds['max_infra_failure_rate_pct']}%`)"
    )
    lines.append(
        f"- Median duration: `{metrics['median_duration_sec']}` sec "
        f"(target <= `{thresholds['max_median_duration_sec']}` sec)"
    )
    lines.append(
        f"- Compose auto-reuse rate: `{metrics['compose_reuse_rate_pct']}%` "
        f"(target >= `{thresholds['min_compose_reuse_rate_pct']}%` when auto policy runs exist)"
    )
    lines.append("")
    lines.append("### Check Results")
    for check in checks:
        lines.append(
            f"- `{check['name']}`: `{check['status']}` (actual `{check['actual']}`, target `{check['target']}`)"
        )
    if failure_classes:
        lines.append("")
        lines.append("### Failure Class Counts")
        for key in sorted(failure_classes):
            lines.append(f"- `{key}`: `{failure_classes[key]}`")
    return "\n".join(lines) + "\n"


def main() -> int:
    args = parse_args()

    report_paths = collect_report_paths(args.report)
    loaded_reports: list[dict[str, Any]] = []
    missing_reports: list[str] = []

    for path in report_paths:
        if not path.exists():
            missing_reports.append(str(path))
            continue
        data = load_json(path)
        if not isinstance(data, dict):
            continue
        loaded_reports.append(data)

    loaded_runs_total = len(loaded_reports)
    malformed_runs = 0
    runs_total = 0
    gate_fail_runs = 0
    infra_fail_runs = 0
    durations: list[float] = []
    compose_auto_runs = 0
    compose_auto_reuse_runs = 0
    failure_classes: dict[str, int] = {}

    for report in loaded_reports:
        summary = report.get("summary")
        if not isinstance(summary, dict):
            malformed_runs += 1
            continue

        results = report.get("results")
        if not isinstance(results, list):
            malformed_runs += 1
            continue

        runs_total += 1

        if bool(summary.get("gate_failed", False)):
            gate_fail_runs += 1

        duration_value = summary.get("duration_sec")
        if isinstance(duration_value, (int, float)):
            durations.append(float(duration_value))

        if summary.get("compose_build_policy") == "auto":
            compose_auto_runs += 1
            if summary.get("compose_build_reason") == "auto_reuse":
                compose_auto_reuse_runs += 1

        run_has_infra_failure = False
        for item in results:
            if not isinstance(item, dict):
                continue
            if item.get("status") != "fail":
                continue
            failure_class = item.get("failure_class")
            if isinstance(failure_class, str) and failure_class.strip():
                key = failure_class.strip()
                failure_classes[key] = failure_classes.get(key, 0) + 1
                if key == "infra_error":
                    run_has_infra_failure = True
        if run_has_infra_failure:
            infra_fail_runs += 1

    gate_failure_rate_pct = pct(gate_fail_runs, runs_total)
    infra_failure_rate_pct = pct(infra_fail_runs, runs_total)
    compose_reuse_rate_pct = pct(compose_auto_reuse_runs, compose_auto_runs)
    median_duration_sec = (
        round(statistics.median(durations), 3) if durations else None
    )

    checks: list[dict[str, Any]] = []
    checks.append(
        {
            "name": "min_runs",
            "status": "pass" if runs_total >= args.min_runs else "fail",
            "actual": runs_total,
            "target": f">= {args.min_runs}",
        }
    )
    checks.append(
        {
            "name": "gate_failure_rate",
            "status": "pass"
            if gate_failure_rate_pct <= args.max_gate_failure_rate_pct
            else "fail",
            "actual": f"{gate_failure_rate_pct}%",
            "target": f"<= {args.max_gate_failure_rate_pct}%",
        }
    )
    checks.append(
        {
            "name": "infra_failure_rate",
            "status": "pass"
            if infra_failure_rate_pct <= args.max_infra_failure_rate_pct
            else "fail",
            "actual": f"{infra_failure_rate_pct}%",
            "target": f"<= {args.max_infra_failure_rate_pct}%",
        }
    )
    if median_duration_sec is None:
        checks.append(
            {
                "name": "median_duration",
                "status": "fail",
                "actual": "missing",
                "target": f"<= {args.max_median_duration_sec}",
            }
        )
    else:
        checks.append(
            {
                "name": "median_duration",
                "status": "pass"
                if median_duration_sec <= args.max_median_duration_sec
                else "fail",
                "actual": median_duration_sec,
                "target": f"<= {args.max_median_duration_sec}",
            }
        )
    if compose_auto_runs == 0:
        checks.append(
            {
                "name": "compose_reuse_rate",
                "status": "pass",
                "actual": "n/a",
                "target": f">= {args.min_compose_reuse_rate_pct}% when auto-policy runs exist",
            }
        )
    else:
        checks.append(
            {
                "name": "compose_reuse_rate",
                "status": "pass"
                if compose_reuse_rate_pct >= args.min_compose_reuse_rate_pct
                else "fail",
                "actual": f"{compose_reuse_rate_pct}%",
                "target": f">= {args.min_compose_reuse_rate_pct}%",
            }
        )

    ready = all(check["status"] == "pass" for check in checks)
    readiness_report = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "ready": ready,
        "inputs": {
            "report_paths": [str(path) for path in report_paths],
            "missing_reports": missing_reports,
            "loaded_runs": loaded_runs_total,
            "malformed_runs": malformed_runs,
            "analyzed_runs": runs_total,
        },
        "thresholds": {
            "min_runs": args.min_runs,
            "max_gate_failure_rate_pct": args.max_gate_failure_rate_pct,
            "max_infra_failure_rate_pct": args.max_infra_failure_rate_pct,
            "max_median_duration_sec": args.max_median_duration_sec,
            "min_compose_reuse_rate_pct": args.min_compose_reuse_rate_pct,
        },
        "metrics": {
            "runs_total": runs_total,
            "gate_fail_runs": gate_fail_runs,
            "gate_failure_rate_pct": gate_failure_rate_pct,
            "infra_fail_runs": infra_fail_runs,
            "infra_failure_rate_pct": infra_failure_rate_pct,
            "durations_observed": len(durations),
            "median_duration_sec": median_duration_sec,
            "compose_auto_runs": compose_auto_runs,
            "compose_auto_reuse_runs": compose_auto_reuse_runs,
            "compose_reuse_rate_pct": compose_reuse_rate_pct,
        },
        "failure_classes": failure_classes,
        "checks": checks,
    }

    out_json = Path(args.out_json)
    out_md = Path(args.out_md)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_md.parent.mkdir(parents=True, exist_ok=True)

    with out_json.open("w", encoding="utf-8") as f:
        json.dump(readiness_report, f, indent=2, ensure_ascii=True)
        f.write("\n")

    markdown = build_markdown(readiness_report)
    with out_md.open("w", encoding="utf-8") as f:
        f.write(markdown)

    sys.stdout.write(
        "ci parity readiness: "
        f"ready={str(ready).lower()} runs={runs_total} "
        f"gate_failure_rate={gate_failure_rate_pct}% "
        f"infra_failure_rate={infra_failure_rate_pct}%\n"
    )
    sys.stdout.write(f"json report: {out_json}\n")
    sys.stdout.write(f"markdown summary: {out_md}\n")

    if args.fail_on_unready and not ready:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
