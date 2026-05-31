# Performance Metrics Profile (v1)

`scripts/perf_metrics_profile.py` is the canonical helper for benchmark profile
objects used by release gates.

## Goals

- Stable profile shape across startup/first-call/stressed-path scenarios.
- Bounded cardinality labels (`a-z0-9_.-`, lowercase, max 48 chars).
- Deterministic percentile fields (`p50_ms`, `p95_ms`, `p99_ms`).
- Explicit gate outcome (`gate_pass`) with threshold metadata.

## Profile shape

```json
{
  "profile_version": "v1",
  "scenario": "startup_print_tools",
  "labels": {
    "phase": "startup",
    "transport": "stdio",
    "runtime": "rust"
  },
  "count": 25,
  "error_count": 0,
  "min_ms": 20.0,
  "p50_ms": 21.0,
  "p95_ms": 23.0,
  "p99_ms": 24.0,
  "avg_ms": 21.5,
  "max_ms": 26.0,
  "thresholds": {
    "max_p50_ms": 50.0,
    "max_p95_ms": 100.0,
    "gate_disabled": false
  },
  "gate_pass": true
}
```

## Helper usage

```bash
python3 scripts/perf_metrics_profile.py \
  --scenario startup_print_tools \
  --samples-ms "20,21,22,23" \
  --error-count 0 \
  --max-p50-ms 50 \
  --max-p95-ms 100 \
  --label phase=startup \
  --label transport=stdio \
  --label runtime=rust
```

Exit code:

- `0`: gate passed
- `2`: gate failed
