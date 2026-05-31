# Migration and Rollout Guide (Python -> Rust stdio)

This guide describes a practical migration path from Python `postgres-mcp` to
`postgres-mcp` with parity safeguards.

## Contract references

- current public SQL surface: `README.md`
- legacy v2 fixture contract: `docs/payload-v2-contract.md`
- machine-readable parity fixtures: `fixtures/parity_v2/`

## Current router contract

The current public router is intent-based:

- `query_sql`: structured read queries with stable object-row payloads
- `query_tuples`: structured read queries with compact columns-plus-rows payloads
- `render_sql`: display-first markdown output for agents/operators
- `export_sql`: direct full-result export surface
- `describe_sql`: projected-schema inspection
- `admin_sql`: explicit mutating SQL surface, disabled by default
- `query_job_start` / `export_job_start` / `job_status` / `job_cancel`:
  deprecated compatibility surfaces for async reads and exports

Structured tools return canonical `ok/data/meta` and `ok/error/meta`
envelopes and now advertise `outputSchema`. `render_sql` returns text-only
success output and advertises optional task support. `export_sql`
returns `artifact_handle` and `artifact_uri`, and `artifact_uri` is retrievable
through MCP `resources/list` and `resources/read`.

## Legacy v2 fixture note

`docs/payload-v2-contract.md` remains only as legacy fixture documentation for
the old v2 parity harness. It is not the current public router contract.

## Advisor mode migration

Advisor tools (`analyze_query_indexes`, `analyze_workload_indexes`) default to
`method=dta` and remain parity-focused in that mode.

Optional extension mode:

- `method=external` (disabled by default)
- bounded by timeout + max-attempts
- can fall back to deterministic `dta` when enabled (`fallback_dta=true`)

Provider-neutral policy guard:

- keep `postgres-mcp` free of provider-specific dependencies/config/docs.
- implement provider adapters in external extension packages.

## Legacy to current mapping

| Legacy behavior | Current behavior |
| --- | --- |
| Direct structured SQL reads | `query_sql` |
| Compact structured SQL reads | `query_tuples` |
| Reader-first markdown inspection | `render_sql` |
| Full-result export artifact | `export_sql` |
| Result-schema inspection | `describe_sql` |
| Mutating SQL / DDL | `admin_sql` when explicitly enabled |
| MCP-native async reads | `query_sql` or `query_tuples` with task augmentation |
| MCP-native async exports | `export_sql` with task augmentation |
| Legacy async compatibility | `query_job_start` / `export_job_start` / `job_status` / `job_cancel` |

Recommended agent defaults:

- Use `query_sql` for structured reads.
- Use `query_tuples` when compact columns-plus-rows output is easier for the
  caller to consume than named objects.
- Use `render_sql` when the reader only needs markdown output.
- Use `export_sql` when the consumer needs the full logical result set as an
  artifact.
- Use `describe_sql` before query execution when the agent is uncertain about
  projected columns.
- Prefer task augmentation plus `tasks/get`, `tasks/result`, and `tasks/cancel`
  for long-running reads and exports.
- Leave `admin_sql` disabled unless mutating SQL is explicitly required.

Acceptance gate:

- Run Build Helper preset `postgres-mcp.test`.
- If tool inventory changes intentionally, also run
  `postgres-mcp.test-update-tool-snapshots`.

## Parity caveats to validate

1. Extension-dependent paths
- `get_top_queries`, `analyze_workload_indexes`, and `explain_query` with
  `hypothetical_indexes` require extension readiness and can return
  `EXTENSION_UNAVAILABLE`.

2. Restricted SQL enforcement
- Rust restricted mode enforces read-only classification and can reject queries
  with `FORBIDDEN_KEYWORD`, `NOT_READ_ONLY_PREFIX`, or
  `EXPLAIN_NOT_READ_ONLY`.

3. Known differences
- Review `fixtures/parity_v2/known_differences.json` before migration.
- Any new difference must include rationale and compatibility impact.

## Recommended rollout sequence

1. Shadow validation
- Run semantic parity and integration matrix checks against representative data.
- Validate representative client workflows against the current public SQL tools.

2. Canary routing
- Route low-risk traffic first.
- Monitor error code distribution and latency profiles.

3. Full cutover
- Promote Rust endpoint.
- Keep Python artifact available for immediate rollback.

## Verification commands

```bash
./scripts/contract_parity_check.sh
./scripts/parity_manifest_check.sh
./scripts/parity_semantic_diff.sh
./scripts/integration_matrix_check.sh --with-compose
./scripts/sql_policy_conformance_diff.sh
./scripts/runtime_safety_conformance.sh --require-db-runtime
./scripts/cold_start_bench.sh
```

## Rollback decision points

Rollback if any of the following occur:

- high-severity parity regressions
- repeated extension probe failures
- enforced performance gates fail

Rollback action:

1. switch traffic back to Python server
2. attach parity/performance reports to incident
3. open a follow-up issue with exact failing scenario IDs and code/reason data
