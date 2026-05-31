# Reliability Exercises

This program keeps startup, discovery, and backpressure controls operational
through scheduled exercises, evidence capture, and remediation closure.

## Cadence

- Frequency: monthly or before high-risk release promotion.
- Participants: maintainers responsible for runtime, database, and release
  approval.
- Environment: production-like staging with representative PostgreSQL topology.
- Timebox: 60-90 minutes per exercise plus a short retrospective.

## Required Scenarios

Run at least one scenario from each class every quarter:

1. Startup lease contention and split-ownership prevention.
2. Dependency validation degradation with operator recovery.
3. Retry pressure and per-tool circuit-breaker opening.
4. Metadata discovery burst with backpressure envelope validation.

## Scenario A: Startup Split-Ownership Prevention

Inject overlapping startup attempts for the same `lease_key`.

Expected:

- one lease holder at a time
- deterministic phase journal events
- no ambiguous partial ownership in logs

Pass criteria:

- non-holder startup attempts fail or wait deterministically
- no schema corruption or inconsistent phase status occurs

## Scenario B: Degraded Read-Only Dependency Mode

Remove or rename one relation listed in
`POSTGRES_MCP_STARTUP_REQUIRED_RELATIONS`.

Expected:

- startup enters `degraded_read_only` when
  `POSTGRES_MCP_STARTUP_DEPENDENCY_MODE=degrade-read-only`
- v2 envelopes include capability state and missing dependencies
- write-unsafe SQL is denied with deterministic code/reason

## Scenario C: Retry Pressure and Circuit Opening

Trigger repeated retryable failures for a high-cost tool, such as `execute_sql`
with induced timeout or connection failures.

Expected:

- consecutive retryable failures trip the per-tool circuit breaker
- responses return `TOOL_CIRCUIT_OPEN`
- `meta.backpressure.retry_after_ms` and backoff guidance are present

## Scenario D: Metadata Discovery Burst

Run concurrent `list_objects` and `get_object_details` bursts while backend
conditions are unstable.

Expected:

- bounded error surfaces with stable fingerprint and retryability guidance
- circuit and backpressure behavior remains deterministic
- CPU and memory growth remain bounded

## Evidence Template

- Date/time in UTC
- Environment summary
- Scenario ID and injection method
- Commands executed
- Key response fields: `code`, `reason`, `fingerprint`, `retryable`,
  `meta.capabilities`, and `meta.backpressure`
- Logs or report paths
- Outcome: pass or fail
- Follow-up issue refs for remediation

## Closure Criteria

An exercise is complete when:

1. Evidence is stored in the chosen release or issue tracker.
2. Failures have follow-up issues with maintainer and priority.
3. Remediation status is reviewed before the next high-risk release.
