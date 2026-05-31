# Ownership Ergonomics Kit

This kit is the cold-start package for maintainers responsible for
`postgres-mcp` reliability and incident response.

## 1) Diagnostic surface (single command)

Capture a redaction-safe runtime diagnostic snapshot:

```bash
./scripts/owner_diagnostic_snapshot.sh
```

This emits:

- binary version + path
- tool-schema hash + tool list
- startup reliability modes (coordination/dependency/degradation)
- backpressure/circuit-breaker policy settings
- safe env posture (`database_uri_set=true|false` without exposing secrets)

Use this output as the first attachment on incidents and rollout handoffs.

## 2) Mental model (one-page)

Core runtime stack:

1. `main.rs`: startup orchestration (connect mode, lease coordination,
   dependency validation, degraded-state activation).
2. `server.rs`: tool routing, low-cardinality telemetry, per-tool circuit
   breaker and backpressure metadata.
3. `tools/*`: contract envelopes, query/schema/health behavior.
4. `db.rs`: transport and SQL execution safety boundaries.

Failure posture order:

1. Prevent split ownership (`startup_coordination` lease + fencing).
2. Validate dependency closure (`startup_dependencies`).
3. If configured, degrade safely (`degraded_read_only`) instead of serving
   unsafe write behavior.
4. Under retry storms, open per-tool circuits and return bounded
   `meta.backpressure`.

## 3) Escalation matrix

- `code=TOOL_CIRCUIT_OPEN`:
  - Primary owner: runtime/on-call.
  - Action: inspect retryable-failure source, reduce pressure, wait cooldown,
    confirm close transition.
- `code=STARTUP_DEGRADED_READ_ONLY`:
  - Primary owner: schema/deployment owner.
  - Action: restore required relations, re-run startup validation, confirm
    `meta.capabilities.startup_state=healthy`.
- startup lease acquisition failures:
  - Primary owner: reliability owner.
  - Action: inspect lease table + phase journal, resolve stale owner, rerun.

## 4) Failure fingerprint lookup flow

1. Collect `code`, `reason`, `fingerprint`, `retryable`,
   `meta.capabilities`, and `meta.backpressure`.
2. Map by class:
  - startup integrity (`STARTUP_*`, lease/dependency)
  - runtime load control (`TOOL_CIRCUIT_OPEN`)
  - DB transport/query (`DB_*`, SQLSTATE)
3. Route to owning lane:
  - schema/startup orchestration
  - runtime backpressure/circuit policy
  - DB transport/connectivity

## 5) Owner KPIs

Track monthly:

- `MTTU` (mean time to understand): incident open -> first valid class
  assignment (`code/reason` + lane owner).
  - target: <= 10 minutes.
- `MTTM` (mean time to mitigate): incident open -> safe stabilization
  (`degraded_read_only` or circuit-open containment when needed).
  - target: <= 20 minutes.
- `Exercise freshness`: days since last completed reliability exercise.
  - target: <= 30 days.
- `Remediation closure SLA`: open exercise-derived remediations older than 30 days.
  - target: 0.
- `Contract hygiene`: % incidents with complete evidence tuple
  (`code/reason/fingerprint/capabilities/backpressure`).
  - target: >= 95%.
