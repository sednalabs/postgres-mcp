# Runbook: postgres-mcp

This runbook covers the Rust stdio postgres MCP service rollout from parity
validation through production cutover.

## Preflight

Required:

- Rust toolchain (`cargo`, `rustc`)
- Docker (for matrix checks)
- `DATABASE_URI` for DB-backed performance gates
- Python reference server available for semantic parity harness

Run and verify:

```bash
cargo test
./scripts/contract_parity_check.sh
./scripts/parity_manifest_check.sh
./scripts/parity_semantic_diff.sh
./scripts/index_advisor_repro_check.sh
./scripts/integration_matrix_check.sh --with-compose
./scripts/cold_start_bench.sh
```

Operational fingerprint checks (stdio service evidence):

```bash
./target/debug/postgres-mcp --version
./target/debug/postgres-mcp --print-tools
ps -fp <postgres-mcp-pid> -o pid=,lstart=,cmd=
```

Maintainer diagnostic snapshot (redaction-safe handoff evidence):

```bash
./scripts/owner_diagnostic_snapshot.sh
```

Pass criteria:

- semantic parity report has `failed=0`
- integration matrix has no high-severity failures
- performance report (`.tmp/perf/perf_gate_report.json`) has `gate_pass=true`
  for every enforced scenario

## Rollout stages

1. Shadow
- Keep Python server as active endpoint.
- Run Rust parity + matrix + perf gates against production-like data.
- Compare reports and open an issue for every unresolved mismatch.
- Capture and archive service fingerprint evidence:
  - binary version via `--version`
  - tool schema via `--print-tools`
  - deferred discovery smoke via `./scripts/deferred_tool_discovery_smoke.sh`
  - startup commandline and process start time from `ps -fp`

2. Canary
- Route a low-risk subset of traffic to Rust server.
- Keep Python path hot for immediate fallback.
- Monitor `EXTENSION_UNAVAILABLE`, `EXTENSION_CHECK_FAILED`,
  `FORBIDDEN_KEYWORD`, and transport errors.

3. Cutover
- Promote Rust server to primary.
- Keep Python deployment artifact available for same-day rollback.
- Re-run parity + perf gates after cutover to confirm no environment drift.

## Rollback triggers and actions

Trigger: semantic parity regression on high-severity case
Action: return traffic to Python server; open an incident issue with failing case IDs.

Trigger: repeated `EXTENSION_CHECK_FAILED` or DB probe instability
Action: rollback traffic; validate database health + extension visibility;
re-run integration matrix before reattempting canary.

Trigger: performance gate failure in enforced scenarios
Action: halt rollout; attach `.tmp/perf/perf_gate_report.json` to incident;
rollback to last passing release.

## Triage map

`EXTENSION_UNAVAILABLE`
- Meaning: required extension missing or not installable in target DB.
- Operator action: install/enable extension or disable affected workflow.

`EXTENSION_CHECK_FAILED`
- Meaning: extension probe query failed (permission/network/runtime error).
- Operator action: verify DB connectivity and permissions; inspect DB logs.

`FORBIDDEN_KEYWORD` / `EXPLAIN_NOT_READ_ONLY`
- Meaning: restricted SQL policy rejected mutation/admin SQL.
- Operator action: rewrite request as read-only or switch to unrestricted mode
  for controlled maintenance workflows.

`CAPABILITY_GUARD_UNAVAILABLE`
- Meaning: in-process guard state is unavailable.
- Operator action: restart service, capture logs, and treat as incident if repeated.

## Troubleshooting: MCP Stdio Startup and Handshake Failures

Symptom set from an MCP client:

- `MCP client for postgres failed to start`
- `handshaking with MCP server failed`
- `Transport ... Broken pipe (os error 32), when send initialize request`

Observed stderr fingerprints:

- `postgres-mcp failed to start: failed to connect to PostgreSQL: db error`
- structured tool error with `code=DB_CONNECT_FAILED`, `reason=db_connect_failed`,
  `sqlstate=08P01`

Likely causes:

1. Startup policy mismatch:
- `--startup-db-connect=fail-fast` exits the stdio server before MCP initialize if
  the first DB probe fails.
- This surfaces as a client-side broken pipe during handshake.

2. DB transport mismatch on PgBouncer/managed endpoints:
- Endpoint requires TLS-compatible client transport.
- Rust server with non-TLS/default DSN can report `08P01` while direct `psql` may still
  work under different defaults.

Resolution that was validated in production-like local environment:

1. Keep startup non-blocking for stdio MCP:
- Use `--startup-db-connect=background` in the MCP server args.

2. Use explicit TLS mode in DSN for this endpoint:
- Set `sslmode=require` in `DATABASE_URI`.
- For current compatibility mode, pass `--allow-insecure-tls`.

3. Re-test with probe tooling before declaring fixed:
- Run a stdio handshake probe against the server command with the same args/env
  used by the MCP client (`probe_handshake` path).
- Run a real tool call probe such as `list_schemas` (`probe_call_tool` path).
- Confirm both:
  - handshake reaches initialize/ping/tools-list successfully, and
  - tool call returns schema rows (not `DB_CONNECT_FAILED`).

Generic MCP client config example (redacted, promoted-artifact launcher):

```toml
[mcp_servers.postgres]
command = "/home/<user>/.../postgres-mcp/scripts/launch_postgres_mcp_from_promoted.sh"
args = ["--access-mode=unrestricted", "--startup-db-connect=background", "--allow-insecure-tls"]

[mcp_servers.postgres.env]
DATABASE_URI = "host=<db-host> port=6432 dbname=<db> user=<user> password='<redacted>' sslmode=require"
```

Optional metadata publication before restart (launcher still does not build):

```bash
./scripts/publish_postgres_mcp_artifact.sh ./target/debug/postgres-mcp
```

The launcher always chooses the newest existing local binary from:

- `target/debug/postgres-mcp`
- `target/release/postgres-mcp`

Security follow-up (recommended):

- Prefer `sslmode=verify-full` (or `verify-ca`) with trusted CA roots and remove
  `--allow-insecure-tls` after certificate chain validation is in place.

## Correlation breadcrumbs

When opening incidents, include:

- request timestamp (UTC)
- tool name
- `code` and `reason` fields from tool response
- parity case ID (if reproduced by harness)
- performance scenario ID (if perf related)
- `perf_gate_report.json` and semantic parity report paths

## Reliability Exercises

Recurring reliability exercise requirements and scenario playcards are defined in:

- `docs/reliability-exercises.md`

Before each production cutover window:

1. Confirm an exercise was run in the prior 30 days.
2. Confirm unresolved findings are tracked as issues with maintainers.
3. Confirm startup/degraded/backpressure scenarios are represented in recent
   exercise evidence.

## Rollback rehearsal record

Rehearsal date: 2026-02-06

Commands:

```bash
# enforce DB-backed gates (expected failure without DATABASE_URI)
REQUIRE_DB_SCENARIOS=1 ./scripts/cold_start_bench.sh

# local safe fallback mode (expected pass with DB scenarios recorded as skipped)
REQUIRE_DB_SCENARIOS=0 ./scripts/cold_start_bench.sh
```

Outcome:

- enforced mode failed as expected with explicit DB prerequisite error.
- fallback mode passed and produced a complete report with scenario status.

This confirms rollback/fallback decision points are executable and observable.
