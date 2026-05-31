# RFC: Cross-Service Architecture Alignment for Policy, Errors, and Contract Guards

## Status

- Date: 2026-02-06
- Status: accepted (phase-2 implementation baseline)
- Scope: `postgres-mcp` plus peer Rust MCP services with comparable policy and contract boundaries

## Why this RFC exists

The four Rust MCP services have converged on similar goals (stable policy decisions,
actionable errors, contract/capability checks, low-noise observability), but still
implement those surfaces differently.

The differences are not failures, but they increase migration and maintenance cost.
This RFC defines one canonical profile and a phased path to converge.

## Design principles

1. Policy logic should be deterministic and side-effect free.
2. Error contracts should be stable and machine-readable.
3. Capability/contract guards should fail closed and remain lazy by default.
4. Observability should expose comparable percentiles and error rates without payload leakage.

## Current-state mapping

| Surface | postgres-mcp | peer auth-boundary service | peer contract service | peer admin gateway service |
|---|---|---|---|---|
| Policy core | local SQL restricted-mode classifier (`src/sql_safety.rs`) | service policy module (`src/policy.rs`) | auth + contract checks split across modules | auth + guard logic split by tool family |
| Error envelope | mostly `{ "error": ... }`, plus targeted `code/reason` in some paths | typed API errors with stable code/reason + request id | structured tool errors include code and request id | typed error layer + metrics/audit context |
| Contract/capability guard | extension checks in tool handlers | endpoint + sql-key allowlist and contract endpoints | required signature checks with cache-like behavior | startup and route-level preflight checks |
| Metrics profile | benchmark scripts + event logs; no unified profile schema yet | service metrics focused on auth and DB phases | in-memory p50/p95/p99 model per tool | in-memory counters/histograms, tool status and duration |

## Canonical target profile

### 1. Policy decision contract

Every policy guard should resolve to this semantic shape before presentation-layer mapping:

```json
{
  "allow": true,
  "code": null,
  "reason": null,
  "message": null,
  "details": {}
}
```

For deny:

```json
{
  "allow": false,
  "code": "STABLE_MACHINE_CODE",
  "reason": "stable_reason_token",
  "message": "operator-actionable message",
  "details": {"context": "safe, non-secret"}
}
```

Rules:
- `code` and `reason` are stability commitments.
- `message` can evolve for clarity but must remain redacted.
- `details` is optional and must never carry credentials/tokens/raw SQL secrets.

### 2. Error envelope contract (tool-facing)

Canonical tool error payload:

```json
{
  "status": "error",
  "code": "STABLE_MACHINE_CODE",
  "reason": "stable_reason_token",
  "error": "human-readable summary",
  "request_id": "optional-correlation-id",
  "details": {}
}
```

Rules:
- All services may keep transport-specific wrappers, but this object is the
  required inner contract for cross-service operability.
- If `request_id` is unavailable, set it to `null`.

### 3. Contract/capability guard profile

Guard behavior should be:
- lazy by default (no cold-start blocking unless explicitly configured)
- fail closed for mandatory capabilities
- cache positive checks when safe
- emit stable deny code/reason on failure

Canonical mandatory capability failure code for this phase:
- `EXTENSION_UNAVAILABLE` with service-specific `reason` values

### 4. Observability profile

Per scenario/tool profile fields:
- `count`
- `error_count`
- `p50_ms`
- `p95_ms`
- `p99_ms`
- `mean_ms`
- `stddev_ms`

All benchmark and in-process summaries should be able to render this profile.

## Toolkit extraction boundary rules

Extract into toolkit crate when all are true:
1. logic is deterministic and domain-agnostic,
2. at least two services can use it without behavior forks,
3. API can be versioned without binding to one service schema.

Keep service-local when any is true:
1. behavior depends on service-specific business policy,
2. data model is service-owned,
3. extraction would require hidden coupling to transport/storage internals.

## Concrete decisions for phase-2

1. Create `mcp-toolkit-policy-core` for deterministic policy primitives.
2. Migrate postgres restricted SQL classifier to policy-core.
3. Add capability guard primitive in `mcp-toolkit-policy-runtime` and adopt for postgres extension checks.
4. Keep service-specific contract SQL signature probing service-local for now (depends on schema ownership).
5. Standardize performance profile output for startup/first-call/stressed-path gates.

## Compatibility policy

- Existing client-visible envelopes are preserved during migration where needed.
- Internal policy structures may change, but `code/reason` semantics must remain stable.
- Any intentional divergence requires a documented known-difference entry and migration note.

## Risks and mitigations

1. Risk: over-generalizing policy crate too early.
   Mitigation: keep `policy-core` pure and place stateful helpers in `policy-runtime`.
2. Risk: hidden cold-start regressions from new checks.
   Mitigation: keep checks lazy; enforce startup and first-call gates.
3. Risk: error taxonomy drift across services.
   Mitigation: enforce canonical envelope fields and review in release checklist.

## Phased rollout

1. Extract `policy-core` and migrate SQL restricted mode.
2. Adopt capability guards for extension readiness.
3. Unify performance profile output and SLO gates.
4. Harmonize runbook, safety, and release process.
5. Run an editorial audit for neutral collaborative wording in parity docs.

## Verification requirements

- Policy-core unit tests green.
- Postgres parity and integration harnesses green.
- Performance gate suite includes startup, first-call, stressed path with thresholds.
- Release docs include canonical error and rollback guidance.
