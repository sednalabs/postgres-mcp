# Performance Gate Compliance

This gate suite enforces three latency profiles:

1. `startup_print_tools` (`p50 <= 50ms`, `p95 <= 100ms`)
2. `first_call_sql_probe` (`p50 <= 250ms`, `p95 <= 500ms`)
3. `stressed_path_sql_probe` (`p50 <= 900ms`, `p95 <= 1800ms`)

All three scenarios emit canonical profile objects with:

- `count`
- `error_count`
- `min_ms`
- `p50_ms`
- `p95_ms`
- `p99_ms`
- `avg_ms`
- `max_ms`
- `thresholds`
- `gate_pass`

## Run command

```bash
./scripts/cold_start_bench.sh
```

The script writes a comparable report to:

```text
.tmp/perf/perf_gate_report.json
```

The report includes environment metadata (host/kernel/rustc version, binary hash,
run count, DB-gate mode).

## DB-backed scenarios

`first_call_sql_probe` and `stressed_path_sql_probe` require `DATABASE_URI`.

- Default: `REQUIRE_DB_SCENARIOS=1` (missing `DATABASE_URI` is a gate failure).
- Optional local-only mode: `REQUIRE_DB_SCENARIOS=0` (DB scenarios are skipped but recorded).

## Optional baseline

Set `PYTHON_BASELINE_CMD` to include a non-gating Python startup baseline profile
in the same report.

## Last recorded results

Recorded on 2026-02-06 (local run in this environment):

- Startup profile passed (`p50=21ms`, `p95=23ms`, `p99=24ms`).
- DB-backed scenarios were skipped (`REQUIRE_DB_SCENARIOS=0`, `DATABASE_URI` not configured).
