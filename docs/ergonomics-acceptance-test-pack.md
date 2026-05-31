# Ergonomics Acceptance Test Pack

This pack defines acceptance checks for the Postgres MCP agent ergonomics
workflow.

## Scope

- Profiles: `fast_agent`, `human_debug`, `heavy_view`
- Payload shaping: `output_mode=data_only`
- Preflight validation: `preflight_check`
- Long-run UX: `query_start_and_wait`
- Policy hardening: expanded `statement_timeout_ms` cap and heavy-view defaults
- Operator verification loops for provider-neutral status, history, and queue
  checks

## Required automated checks

Run from the repository root:

```bash
cargo test -q
```

This command includes the following focused assertions:

- `resolve_execute_sql_profile_fast_agent_applies_low_overhead_defaults`
- `resolve_execute_sql_profile_human_debug_defaults_count_mode_and_preflight`
- `resolve_execute_sql_profile_heavy_view_respects_explicit_overrides`
- `resolve_execute_sql_profile_human_debug_legacy_include_total_row_count_overrides_default`
- `apply_execute_sql_data_only_compaction_reduces_meta_surface`
- `query_start_and_wait_until_terminal_returns_job_response`
- tool schema snapshot contract stability (`spec/tool_schema_snapshot.v1.json`)

## Operator Verification Loop Matrix

Public docs stay provider-neutral. The executable regression examples below use
single-statement `VALUES`/CTE fixtures that stand in for downstream status,
history, and queue surfaces exercised by operator workflows.

| Loop class | Representative query shape | Preferred invocation shape | Expected output | Failure budget |
| --- | --- | --- | --- | --- |
| Provider triage | grouped provider/status summary for the latest verification slice | `{ "sql": "...", "max_rows": 25, "output_mode": "table" }` | readable object rows (`meta.effective_output_mode=rows`) | `0` correction loops |
| Direct-route date counts | single aggregate count for a read-only date/status check | `{ "sql": "..." }` | compact `data_only` payload | `0` correction loops |
| Queue-state verification | recent queue rows ordered by timestamp for rapid polling | `{ "sql": "...", "max_rows": 25, "profile": "fast_agent" }` | compact `data_only` payload | `0` correction loops |
| Landed-date diffing | joined before/after landed-date comparison with deterministic ordering | `{ "sql": "...", "max_rows": 25, "output_mode": "table" }` | readable object rows (`meta.effective_output_mode=rows`) | `0` correction loops |
| Bound-params correction | first attempt uses params with multi-statement SQL, second attempt rewrites to one statement | fail once with structured invalid-request guidance, then retry | correction hint must point to removing top-level semicolons | `<= 1` correction loop |

## Optional live-db acceptance checks

When `DATABASE_URI` is available, run:

```bash
cargo test -q execute_sql_preflight_missing_relation_returns_structured_error
```

This confirms `preflight_check=true` returns structured preflight errors for
missing-relation paths before execution.

When the local probe binary and validation database are available, run the
operator-loop regression pack:

```bash
python3 ./scripts/ergonomics_rollout_validation.py
```

Equivalent Build Helper preset:

```text
postgres-mcp.ergonomics-rollout-validation
```

This exercises the provider-neutral workflow matrix above, including the
single-statement `params` correction lane.

## Rollout verification notes

- Confirm downstream callers use `sql`-only or `output_mode=table` for
  short read-only verification loops and reserve `profile=fast_agent` for
  repeated polling paths.
- Confirm the rollout validation fixture keeps the four workflow loops at
  `0` correction loops and the bound-params correction lane at `<= 1`.
- Confirm heavy analytical callers switch to `profile=heavy_view` (or explicit
  `statement_timeout_ms` where needed).
- Confirm operators use `query_start_and_wait` for one-call long-running flows.
