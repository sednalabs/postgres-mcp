#!/usr/bin/env python3
"""Compute a canonical performance profile and evaluate gate thresholds."""

from __future__ import annotations

import argparse
import json
import re
import statistics
import sys
from typing import Dict, List, Optional

LABEL_MAX_LEN = 48
UNKNOWN_LABEL = "unknown"
PROFILE_VERSION = "v1"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a canonical perf profile and enforce p50/p95 gates."
    )
    parser.add_argument("--scenario", required=True, help="Scenario name")
    parser.add_argument(
        "--samples-ms",
        default="",
        help="Comma-separated list of integer/float milliseconds",
    )
    parser.add_argument("--error-count", type=int, default=0)
    parser.add_argument(
        "--label",
        action="append",
        default=[],
        help="Label in key=value format (repeatable)",
    )
    parser.add_argument("--max-p50-ms", type=float, default=None)
    parser.add_argument("--max-p95-ms", type=float, default=None)
    parser.add_argument(
        "--gate-disabled",
        action="store_true",
        help="Emit profile without threshold enforcement.",
    )
    return parser.parse_args()


def normalize_label_value(raw: str) -> str:
    lowered = raw.strip().lower()
    if not lowered:
        return UNKNOWN_LABEL
    normalized = re.sub(r"[^a-z0-9_.-]+", "_", lowered).strip("_")
    if not normalized:
        normalized = UNKNOWN_LABEL
    return normalized[:LABEL_MAX_LEN]


def normalize_label_key(raw: str) -> str:
    return normalize_label_value(raw)


def parse_labels(raw_labels: List[str]) -> Dict[str, str]:
    labels: Dict[str, str] = {}
    for item in raw_labels:
        if "=" not in item:
            continue
        key, value = item.split("=", 1)
        labels[normalize_label_key(key)] = normalize_label_value(value)
    return labels


def parse_samples(raw: str) -> List[float]:
    values: List[float] = []
    for part in raw.split(","):
        token = part.strip()
        if not token:
            continue
        try:
            values.append(float(token))
        except ValueError:
            continue
    values.sort()
    return values


def percentile(sorted_values: List[float], p: float) -> Optional[float]:
    if not sorted_values:
        return None
    if p <= 0:
        return sorted_values[0]
    if p >= 1:
        return sorted_values[-1]
    idx = int((len(sorted_values) - 1) * p + 0.999999)
    return sorted_values[idx]


def gate_pass(
    p50_ms: Optional[float],
    p95_ms: Optional[float],
    error_count: int,
    max_p50_ms: Optional[float],
    max_p95_ms: Optional[float],
    gate_disabled: bool,
) -> bool:
    if gate_disabled:
        return True
    if p50_ms is None or p95_ms is None:
        return False
    if error_count > 0:
        return False
    if max_p50_ms is None or max_p95_ms is None:
        return False
    return p50_ms <= max_p50_ms and p95_ms <= max_p95_ms


def main() -> int:
    args = parse_args()
    samples = parse_samples(args.samples_ms)
    labels = parse_labels(args.label)

    p50 = percentile(samples, 0.50)
    p95 = percentile(samples, 0.95)
    p99 = percentile(samples, 0.99)
    avg = statistics.fmean(samples) if samples else None
    min_ms = samples[0] if samples else None
    max_ms = samples[-1] if samples else None

    passed = gate_pass(
        p50_ms=p50,
        p95_ms=p95,
        error_count=max(args.error_count, 0),
        max_p50_ms=args.max_p50_ms,
        max_p95_ms=args.max_p95_ms,
        gate_disabled=args.gate_disabled,
    )

    profile = {
        "profile_version": PROFILE_VERSION,
        "scenario": normalize_label_value(args.scenario),
        "labels": labels,
        "count": len(samples),
        "error_count": max(args.error_count, 0),
        "min_ms": min_ms,
        "p50_ms": p50,
        "p95_ms": p95,
        "p99_ms": p99,
        "avg_ms": avg,
        "max_ms": max_ms,
        "thresholds": {
            "max_p50_ms": args.max_p50_ms,
            "max_p95_ms": args.max_p95_ms,
            "gate_disabled": args.gate_disabled,
        },
        "gate_pass": passed,
    }

    sys.stdout.write(json.dumps(profile, separators=(",", ":")))
    return 0 if passed else 2


if __name__ == "__main__":
    raise SystemExit(main())
