# Payload Contract v2

This document records the v2 response contract for the `execute_sql` /
`query_*` SQL surface.

The current public router also exposes the intent-oriented helpers alongside
that contract-driven path:

- `execute_sql`
- `query_start`
- `query_start_and_wait`
- `query_status`
- `query_cancel`
- `query_sql`
- `query_tuples`
- `render_sql`
- `describe_sql`
- `admin_sql`
- `session_open`
- `session_status`
- `session_close`
- `query_job_start`
- `export_job_start`
- `job_status`
- `job_cancel`

Use this file for contract reference, fixture interpretation, and migration
guidance around the v2-shaped SQL path.

## Scope

This payload contract applies to responses produced by the v2-shaped SQL
surface.

- Success responses use the `ok/data/meta` envelope.
- Error responses use the `ok/error/meta` envelope.

## Canonical Envelope

### Success envelope

```json
{
  "ok": true,
  "data": {},
  "meta": {
    "elapsed_ms": 12
  }
}
```

### Error envelope

```json
{
  "ok": false,
  "error": {
    "error": "Error executing query: permission denied for relation pg_authid",
    "code": "QUERY_EXECUTION_FAILED",
    "reason": "query_execution_failed",
    "sqlstate": "42501",
    "detail": null,
    "hint": null,
    "position": null
  },
  "meta": {
    "elapsed_ms": 6
  }
}
```

## Field Semantics

### Top-level fields

- `ok` (`boolean`, required): success/failure discriminator.
- `data` (`any`, required when `ok=true`): tool payload.
- `error` (`object`, required when `ok=false`): structured failure payload.
- `meta` (`object`, required): response metadata.

### Shared meta fields

- `elapsed_ms` (`number`, required): wall-clock execution time in milliseconds.
- `capabilities` (`object`, optional): explicit runtime capability contract for startup health posture. Compact successful responses may omit this field when startup state is healthy:
  - `startup_state` (`healthy|degraded_read_only`)
  - `degraded_read_only` (`boolean`)
  - `read_only_sql` (`boolean`)
  - `read_write_sql` (`boolean`)
  - `metadata_discovery` (`boolean`)
  - `reason` (`string|null`)
  - `missing_dependencies` (`array<string>`)

### Error payload notes

- `error.hint` may be `null` or a string.
- `error.schema_hints` may be `null` or a structured object with one of these
  shapes:
  - missing relation:
    `{ "kind": "missing_relation", "missing_relation": string, "similar_relations": array<string>, "discovery": object|null }`
  - missing FROM alias:
    `{ "kind": "missing_from_alias", "missing_alias": string, "referenced_relations": array<string> }`
  - missing column:
    `{ "kind": "missing_column", "missing_column": string, "similar_columns": array<string>, "relation_columns": array<object>, "metadata_policy": "available|denied" }`
- For `execute_sql` errors with `sqlstate=42P01` (missing relation),
  `sqlstate=42703` (missing column), or timeout cancellations
  (`sqlstate=57014` with statement-timeout detail), the server may provide an
  MCP-generated fallback hint when PostgreSQL does not return one. These hints
  include alias-scope diagnostics and actionable discovery guidance rather than
  generic retry text.
- Metadata discovery policy denials use
  `code=METADATA_ACCESS_DENIED`, `reason=metadata_access_denied`.
- Runtime startup-role DDL guard denials use
  `code=RUNTIME_ROLE_DDL_BLOCKED`, `reason=startup_role_runtime`.
- Startup degraded read-only denials use
  `code=STARTUP_DEGRADED_READ_ONLY`, `reason=startup_degraded_read_only`.
- Per-tool circuit-open denials use
  `code=TOOL_CIRCUIT_OPEN`, `reason=circuit_breaker_open`.

### Tabular response meta fields

For tabular outputs (notably `execute_sql`) using `output_mode != data_only`,
`meta` includes:

- `output_mode` (`auto|rows|rows_safe|tuples|scalar|data_only`, required):
  output representation mode.
- `summary_only` (`boolean`, required):
  when `true`, payload rows are omitted from `data` and only metadata is
  returned.
- `row_count_mode` (`string`, required):
  row-count strategy used by the server for this response:
  - `page_window`: look-ahead pagination (no explicit count query)
  - `count_exact`: explicit `COUNT(*)` path
  - `count_estimated`: planner-estimated count path
  - `count_async`: asynchronous count job path
- `row_count_total` (`number`, required):
  total rows for the logical query result set when available from the active
  count strategy. `count_exact` returns exact totals, `count_estimated` returns
  planner-estimated totals, and `count_async` may initially return the page
  total until the async count job completes.
- `row_count_returned` (`number`, required):
  rows returned in this response payload.
- `has_more` (`boolean`, required):
  canonical continuation signal (`true` when `next_cursor` is present).
- `truncated` (`boolean`, required):
  compatibility field aligned with `has_more`.
- `cursor_offset` (`number|null`, required):
  current logical page offset for cursor-capable response paths.
- `next_cursor` (`string|null`, required):
  pagination cursor for the next page; `null` when no additional page exists.
- `next_offset` (`number|null`, required):
  next logical offset when `has_more=true`; otherwise `null`.
- `query_hash` (`string|null`, required):
  stable hash for cursor/query binding checks.
- `execution` (`object`, required):
  nested factual execution descriptor:
  - `contract_version` (`execution/v1`)
  - `scope` (`execute_sql|query_start`)
  - `sql` (`object`): `statement_kind`, `rewritten`, `helper_expansions`
  - `params` (`object`): `bound_count`
  - `timeout` (`object`): `override_applied`
  - `pagination` (`object`): `supported`, `strategy`, `cursor_binding`
  - `count` (`object|null`): factual count execution path when known
- `query_telemetry` (`object`, optional):
  present when `metadata_verbosity=standard|full`. Lightweight query telemetry object with:
  - required: `query_hash`, `query_fingerprint`, `elapsed_ms`, `returned_rows`
  - standard/full only: `row_count_mode`, `row_count_total`, `has_more`,
    `cursor_offset`, `next_offset`
- `metadata_verbosity` (`compact|standard|full`, required):
  metadata profile applied to this response. Compatibility alias
  `low -> compact` is accepted on input. `compact` keeps only the core agent loop fields (`output_mode`, `metadata_verbosity`, `query_hash`, timing, returned-row count, pagination/count fields, and export metadata) plus diagnostics that are actually actionable. `standard` keeps `query_hints` but omits `columns`, and `full` preserves all metadata keys.
- `columns` (`array`, optional):
  present when `metadata_verbosity=full`.
  ordered column metadata records:
  `{ "name": string, "pg_type": string, "nullable": boolean|null }`.
  Duplicate selected names are normalized to deterministic aliases
  (`<name>__dupN`) to avoid object-key collisions.
- `column_name_safety` (`object`, optional):
  deterministic duplicate-column metadata:
  - `object_row_safe` (`boolean`)
  - `duplicate_columns_aliased` (`boolean`)
  - `aliased_columns` (`array<string>`)
  - `strategy` (`suffix_alias`)
- `query_hints` (`array`, optional):
  present when `metadata_verbosity=standard|full`.
- `effective_profile` (`string|null`, optional):
  effective profile used for this call (`fast_agent|human_debug|heavy_view` or
  `null`).
- `effective_count_mode` (`none|exact|estimated|async`, optional):
  effective count mode after profile/compatibility resolution.
- `requested_output_mode` (`auto|rows|rows_safe|tuples|scalar|data_only`, optional):
  normalized caller request before runtime auto resolution.
- `effective_output_mode` (`auto|rows|rows_safe|tuples|scalar|data_only`, optional):
  effective output mode after runtime auto resolution.
- `auto_output_resolution` (`object`, optional):
  present when `requested_output_mode=auto`:
  - `requested` (`auto`)
  - `resolved` (`scalar|rows|rows_safe|tuples`)
  - `reason` (`single_cell_result|configured_auto_tabular_default`)
  - `tabular_default` (`rows|rows_safe|tuples`)
- `effective_metadata_verbosity` (`compact|standard|full`, optional):
  effective metadata verbosity after profile/default resolution.
- `export` (`object`, optional):
  present when `export_to_file=true`:
  - `enabled` (`true`)
  - `format` (`csv|tsv|jsonl`)
  - `path` (`string`)
  - `row_count` (`number`)
  - `column_count` (`number`)
  - `bytes` (`number`)
- `backpressure` (`object`, optional):
  incident-aware retry guidance for per-tool circuit-breaker state:
  - `tool` (`string`)
  - `state` (`disabled|closed|open|unknown`)
  - `retry_after_ms` (`number|null`)
  - `consecutive_retryable_failures` (`number`)
  - `policy` (`per_tool_circuit_breaker`)
  - `failure_threshold` (`number`)
  - `cooldown_ms` (`number`)
  - `recommended_backoff_base_ms` (`number`)
  - `recommended_backoff_cap_ms` (`number`)
  - `recommended_backoff_jitter` (`full`)

For `output_mode=data_only`, metadata is intentionally compact and keeps only
the small field set needed for cursor/count loops:

- `output_mode` (`data_only`, required)
- `query_hash` (`string|null`, required)
- `elapsed_ms` (`number|null`, required)
- `truncated` (`boolean`, required)
- `returned_rows` (`number|null`, required)
- `row_count_mode` (`string`, required)
- `row_count_total` (`number`, optional; omitted for `row_count_mode=page_window`)
- `row_count_job_id` (`string`, optional; present for `count_mode=async`)
- `has_more` (`boolean`, required)
- `next_cursor` (`string|null`, required)
- `export` (`object`, optional; same shape as tabular responses)
- `capabilities` (`object`, optional; retained when startup capability state is
  not healthy, omitted on healthy default-success responses)

### Advisor payload fields

`analyze_query_indexes` and `analyze_workload_indexes` return advisor execution
fields inside `data`:

- `method` (`string`, required): effective advisor method (`dta|external`).
- `method_requested` (`string`, required): requested method from tool args.
- `method_effective` (`string`, required): effective path after policy/fallback.
- `fallback_reason` (`string|null`, required): populated when `external`
  requests fall back to `dta`.
- `fallback_message` (`string|null`, required): normalized failure detail used
  for fallback decisions.
- `attempt_count` (`number`, required): bounded advisor attempts executed.
  Can be `0` when no candidate queries are available for analysis.
- `stop_reason` (`string`, required): loop terminal reason
  (`deterministic_single_pass|no_queries|converged|max_attempts|fallback_to_dta|...`).

Additional advisor payload behavior:

- In workload mode, `skipped_queries[].query` may be preview-clipped; when clipped,
  `skipped_queries[].query_truncated=true`.
- In external mode, oversized provider `errors[]` entries are normalized to bounded
  objects with `truncated=true`, `reason`, `original_bytes`, and `preview`.

Provider policy note:

- `postgres-mcp` keeps a provider-neutral public contract.
- Provider-specific implementations belong to external extension packages.

## `execute_sql` Request Controls

`execute_sql` accepts the following pagination and payload controls:

- `max_rows` (`number`, optional):
  per-request page size override (server defaults still apply when omitted).
- `cursor` (`string`, optional):
  opaque continuation cursor from the prior response `meta.next_cursor`.
- `params` (`array`, optional):
  bound parameter values for prepared placeholders (`$1`, `$2`, ...). Raw JSON
  scalars map to PostgreSQL scalar types, homogeneous arrays map to PostgreSQL
  array types, and raw objects/heterogeneous arrays map to `jsonb`/`jsonb[]`.
  Raw `null` and empty arrays are rejected because PostgreSQL type inference is
  ambiguous; use explicit typed wrappers
  `{ "type": "...", "value": ... }` instead. Supported explicit types:
  `bool`, `int8`, `float8`, `text`, `jsonb`, and matching `[]` array forms.
  Wrapper parsing is reserved for ambiguous `null`/array payloads; raw JSON
  objects always bind as `jsonb` even when they contain `type` and `value`
  keys.
  When `params` is present, the SQL must be exactly one top-level statement.
- `max_cell_chars` (`number`, optional):
  clip long string cells in response payloads and report clipping telemetry in
  `meta.cell_clipping`.
- `output_mode` (`auto|rows|rows_safe|tuples|scalar|data_only`, optional):
  output representation for `data`. Alias `table` is accepted as shorthand for
  `rows`, and is the recommended readable-table mode for operational
  verification. Legacy `compact` is intentionally removed; use `data_only`.
- `summary_only` (`boolean`, optional, default `false`):
  when `true`, omit tabular payload rows from `data` while preserving the
  metadata envelope for the selected `output_mode` (for `data_only`, metadata
  remains compact).
- `include_total_row_count` (`boolean|null`, optional, default `null`):
  legacy compatibility bridge resolved after explicit `count_mode`:
  - `true` => `count_mode=exact`
  - `false` => `count_mode=none`
  - `null` => defer to profile/default resolution
  explicit `count_mode` always takes precedence; profile defaults apply only
  when both `count_mode` and `include_total_row_count` are omitted.
- `count_mode` (`none|exact|estimated|async|null`, optional, default `null`):
  canonical count strategy for paginated select-like statements. `null`
  defaults to fast-path `none`.
- `metadata_verbosity` (`compact|standard|full|null`, optional, default `null`):
  controls tabular metadata payload size for `execute_sql`. Compatibility alias
  `low -> compact` is accepted. `null` behaves like `compact` (default). Use
  `full` to preserve legacy-rich metadata fields.
- `profile` (`fast_agent|human_debug|heavy_view|null`, optional, default `null`):
  optional profile defaults:
  - `fast_agent`: `output_mode=data_only`, bounded `max_rows`, bounded cell
    clipping.
  - `human_debug`: `output_mode=rows_safe`, `metadata_verbosity=full`,
    `count_mode=estimated` (unless `count_mode` or `include_total_row_count` is
    explicitly provided), `preflight_check=true`.
  - `heavy_view`: `output_mode=tuples`, `metadata_verbosity=standard`,
    `statement_timeout_ms=300000`, `preflight_check=true`.
  Explicit request args override profile defaults.
- Operational verification happy path:
  - `{ "sql": "..." }` for the default compact `data_only` fetch.
  - `{ "sql": "...", "output_mode": "auto" }` when single-cell aggregates
    should resolve to `scalar`.
  - `{ "sql": "...", "max_rows": 25, "output_mode": "table" }` for readable
    row output.
  - `{ "sql": "...", "max_rows": 25, "output_mode": "json" }` as an alias for
    readable row-object output.
  - `{ "sql": "...", "max_rows": 25, "profile": "fast_agent" }` for compact
    high-frequency verification loops.
- Common operator corrections:
  - invalid `metadata_verbosity`: use `compact|standard|full`
    (`low -> compact` remains accepted)
  - invalid `response_formatting_mode`: use `currency` for currency expansion, `markdown` as a compatibility alias for readable table output, or switch directly to `output_mode=table|rows_safe` / `profile=fast_agent`
  - invalid `output_mode`: canonical `rows` remains preferred, with
    `table -> rows` and `json -> rows` accepted as readability aliases
  - rejected `params` with multi-statement SQL: remove top-level semicolons so
    the parameterized query is exactly one statement
- `statement_timeout_ms` (`number|null`, optional, default `null`):
  per-call statement-timeout override for `execute_sql`. Values must be greater
  than `0` and less than or equal to `300000`. When this override exceeds the
  current request-timeout budget, the request-timeout floor is raised to
  `statement_timeout_ms + 1000ms` for that call.
- `describe_only` (`boolean`, optional, default `false`):
  when `true`, prepare the SQL statement and return result columns/types
  without executing the query body. `describe_only` cannot be combined with
  `export_to_file`.
- `export_to_file` (`boolean`, optional, default `false`):
  when `true`, write the current result page/window to a temp file and report
  file metadata in `meta.export`.
- `export_format` (`csv|tsv|jsonl|null`, optional, default `null`):
  output format for `export_to_file`. `null` resolves to `tsv`.
- `diagnose_on_timeout` (`boolean|null`, optional, default `null`):
  when true, timeout failures include bounded `error.diagnostics` context.
- `preflight_check` (`boolean|null`, optional, default `null`):
  when true, run schema preflight validation before execution. Missing relation
  (`42P01`) and missing column (`42703`) failures return structured
  preflight-specific errors. Preflight accepts only single-statement SQL; when
  top-level delimiters indicate multiple statements, the server returns
  `code=SQL_PREFLIGHT_MULTI_STATEMENT`.

## `execute_sql` Describe-Only Responses

When `describe_only=true`, `execute_sql` returns result-schema metadata instead
of row data:

- `data.columns` (`array`, required):
  ordered column metadata records:
  `{ "name": string, "pg_type": string, "nullable": boolean|null }`
- `meta.describe_only` (`true`, required)
- `meta.query_hash` (`string`, required)
- `meta.column_count` (`number`, required)
- `meta.columns` (`array`, required; same shape as `data.columns`)
- `meta.query_hints` (`array`, required)
- `meta.column_name_safety` (`object`, required; same shape as tabular
  responses)

## Async Query Lifecycle Tools

- `query_start`:
  accepts top-level `ExecuteSqlArgs` fields and returns a `job_id`
  (for example `{ "sql": "...", "max_rows": 100, "count_mode": "none" }`).
  Top-level fields are the canonical shape for automation and generated clients.
  Nested `{ "execute_sql": { ... } }` input is retained only as a compatibility
  fallback when no top-level fields are provided.
  When both shapes are present, top-level fields are authoritative and unmatched
  fields merge from the nested object.
  Launch responses preserve legacy envelope `meta.job_id` / `meta.query_hash`
  and also include `meta.execution` with the same factual execution descriptor
  shape used by `execute_sql`.
- `query_status`:
  accepts `job_id` and optional wait controls:
  - `wait_ms` (bounded deadline wait, minimum `1`),
  - `wait_until_terminal=true` (block until terminal state).
  Omitting `wait_ms` performs an immediate poll. `wait_ms` and
  `wait_until_terminal=true` are mutually exclusive.
- `query_cancel`:
  accepts `job_id` and requests deterministic cancellation.
- `query_start_and_wait`:
  accepts `query_start` args plus optional `wait_ms`; starts a job and waits in
  one call. Omitting `wait_ms` waits until terminal state; `wait_ms=0` is
  invalid and callers should omit the field instead.

`query_status` payload includes canonical lifecycle fields:

- `job_id`
- `query_hash`
- `state` (`pending|running|succeeded|failed|canceled`)
- `terminal` (`boolean`)
- `cancel_requested` (`boolean`)
- timestamps: `created_at_unix_ms`, `started_at_unix_ms`, `finished_at_unix_ms`
- `wait` object: `mode`, `trigger`, `elapsed_ms`, `suggested_wait_ms`
- `progress` object: `kind`, `phase`, `age_ms`, `queue_ms`, `run_ms`,
  `suggested_wait_ms`
- `follow_up` object: `tool`, `suggested_wait_ms`
- terminal snapshots include `response` (final v2 envelope)

## Fast Schema Discovery Pattern

To reduce retry loops before writing ad-hoc SQL, pair `execute_sql` with
`list_objects`:

- Canonical discovery filter modes: `exact|prefix|contains|pattern`.
- Canonical arg-to-mode mapping:
  - `name_exact -> exact`
  - `name_prefix -> prefix`
  - `name_contains -> contains`
  - `name_pattern -> pattern`
- `list_objects.name_like` is legacy-compatible and maps deterministically:
  - no unescaped wildcard (`%`/`_`) -> `contains`
  - unescaped wildcard present -> `pattern`
- Compatibility/deprecation phase policy for legacy inputs is defined in
  `docs/compatibility-lifecycle.md`.
- Exactly one name-filter input is accepted per request.
- `list_objects.limit` requests page size but is clamped by server hard caps.
- `list_objects.cursor` resumes deterministic pagination from `meta.next_cursor`.
- `list_objects.include_columns=true` (for `object_type=table|view`) returns
  inline column arrays for one-call relation+column discovery.

For `list_objects` paginated responses, v2 `meta` includes:

- `returned_rows`
- `has_more`
- `truncated`
- `next_offset`
- `next_cursor`
- `query_hash`
- `limit_requested`
- `limit_effective`
- `limit_hard_cap`
- `offset`
- `column_budget_per_object` (`null` unless `include_columns=true`)
- `metadata_freshness`:
  - `cache_mode` (`direct` in no-cache mode)
  - `cache_status` (`bypass|hit|miss|invalidated`; currently `bypass`)
  - `invalidation_policy` (`schema_version`)
  - `staleness_bound_ms` (`0` in direct mode)
  - `as_of_unix_ms`
  - `schema_name`
  - `schema_version_token` (deterministic invalidation token)

`list_schemas` and `get_object_details` v2 responses also include
`meta.metadata_freshness` with the same contract fields.

## Output Modes

When `output_mode` is set, `data` is shaped as:

- `auto`: resolved at runtime:
  - `scalar` for exactly one row and one column,
  - configured tabular fallback (`rows` by default) otherwise.
- `rows`: array of row objects keyed by column name.
- `rows_safe`: array of row objects with deterministic duplicate-key
  disambiguation (`<name>__dupN`) for collision paths.
- `tuples`: array of positional arrays in result column order.
- `scalar`: first column of first row, or `null` for empty output.
- `data_only`: array of positional tuples with bounded metadata focused on
  `query_hash`, elapsed timing, truncation state, and continuation cursor.

`table` requests are normalized to the canonical `rows` mode in response
metadata (`meta.output_mode: "rows"`). `auto` requests are normalized to the
resolved canonical mode (`meta.output_mode: "scalar"|"rows"|"rows_safe"|"tuples"`).

When `summary_only=true`, `data` is `null` regardless of `output_mode`.

## Pagination and Cursor Contract

Pagination applies to select-like `execute_sql` requests in v2 mode.

- First page: omit `cursor`.
- Next page: pass `cursor=meta.next_cursor` from prior response.
- `meta.has_more` is the canonical continuation boolean; when `false`,
  `meta.next_cursor` is `null`.
- `meta.cursor_offset` and `meta.next_offset` provide deterministic offset
  context for resumable client loops.
- Cursors are versioned, signed, tool-scoped, and time-limited.
- Cursor/query mismatch returns:
  `code=CURSOR_QUERY_MISMATCH`, `reason=invalid_cursor`.
- Expired cursor returns:
  `code=CURSOR_EXPIRED`, `reason=invalid_cursor`.
- Invalid cursor syntax returns:
  `code=INVALID_CURSOR`, `reason=invalid_cursor`.

Clients should treat `next_cursor` as opaque.

## Error Envelope Policy

In v2 mode, error payloads include deterministic diagnostics metadata:

- `fingerprint` (`string`, required): stable class identifier (for example
  `err_ab12cd34ef56`) derived from `code/reason/sqlstate`.
- `retryable` (`boolean`, required): coarse retry guidance for callers.
- `detail_level` (`minimal|detailed`, required): exposure policy used for this
  response.

Role-aware detail policy:

- `startup_role=runtime` emits `detail_level=minimal` and redacts
  high-sensitivity internals (`detail`, `position`).
- `startup_role=migrator` emits `detail_level=detailed` and retains full
  diagnostics fields.

Observability policy note:

- Runtime telemetry is redaction-first and low-cardinality by default.
- Raw error text is excluded from normal tool error events.
- Optional debug previews require explicit opt-in (`POSTGRES_MCP_TELEMETRY_DEBUG=1`)
  and remain clipped to bounded length.

## Normative Examples

### Success: non-truncated tabular page

```json
{
  "ok": true,
  "data": [
    { "schema_name": "public" },
    { "schema_name": "information_schema" }
  ],
  "meta": {
    "elapsed_ms": 5,
    "output_mode": "rows",
    "summary_only": false,
    "row_count_total": 2,
    "row_count_returned": 2,
    "has_more": false,
    "truncated": false,
    "cursor_offset": 0,
    "next_cursor": null,
    "next_offset": null,
    "query_hash": "2c5b5f3f9e8f0b2a",
    "query_telemetry": {
      "query_hash": "2c5b5f3f9e8f0b2a",
      "query_fingerprint": "qf_7d4f88bfdf1792f3",
      "elapsed_ms": 5,
      "returned_rows": 2
    },
    "columns": [
      { "name": "schema_name", "pg_type": "unknown", "nullable": null }
    ]
  }
}
```

### Success: truncated page with cursor

```json
{
  "ok": true,
  "data": [
    { "id": 1 },
    { "id": 2 }
  ],
  "meta": {
    "elapsed_ms": 8,
    "output_mode": "rows",
    "summary_only": false,
    "row_count_total": 5,
    "row_count_returned": 2,
    "has_more": true,
    "truncated": true,
    "cursor_offset": 0,
    "next_cursor": "opaque_cursor_token_page_2",
    "next_offset": 2,
    "query_hash": "2c5b5f3f9e8f0b2a",
    "query_telemetry": {
      "query_hash": "2c5b5f3f9e8f0b2a",
      "query_fingerprint": "qf_b12c4f2b11d39e70",
      "elapsed_ms": 8,
      "returned_rows": 2
    },
    "columns": [
      { "name": "id", "pg_type": "unknown", "nullable": null }
    ]
  }
}
```

### Success: summary-only diagnostics page

```json
{
  "ok": true,
  "data": null,
  "meta": {
    "elapsed_ms": 9,
    "output_mode": "rows",
    "summary_only": true,
    "row_count_total": 200,
    "row_count_returned": 25,
    "has_more": true,
    "truncated": true,
    "cursor_offset": 0,
    "next_cursor": "opaque_cursor_token_page_25",
    "next_offset": 25,
    "query_hash": "2c5b5f3f9e8f0b2a",
    "query_telemetry": {
      "query_hash": "2c5b5f3f9e8f0b2a",
      "query_fingerprint": "qf_2a5e98f2f8ec84cd",
      "elapsed_ms": 9,
      "returned_rows": 25,
      "row_count_mode": "count_exact",
      "row_count_total": 200,
      "has_more": true,
      "cursor_offset": 0,
      "next_offset": 25
    },
    "metadata_verbosity": "full",
    "columns": [
      { "name": "id", "pg_type": "unknown", "nullable": null }
    ],
    "cell_clipping": {
      "enabled": true,
      "max_cell_chars": 40,
      "clipped_cells": 3,
      "applied": true
    },
    "query_hints": []
  }
}
```

### Error: SQLSTATE-first structured DB failure

```json
{
  "ok": false,
  "error": {
    "error": "Error executing query: relation \"missing_table\" does not exist",
    "code": "QUERY_EXECUTION_FAILED",
    "reason": "query_execution_failed",
    "sqlstate": "42P01",
    "detail_level": "minimal",
    "retryable": false,
    "fingerprint": "err_4ad7f3c97b21",
    "hint": null
  },
  "meta": {
    "elapsed_ms": 4
  }
}
```

### Error: cursor/query mismatch

```json
{
  "ok": false,
  "error": {
    "error": "Cursor does not match query hash",
    "code": "CURSOR_QUERY_MISMATCH",
    "reason": "invalid_cursor"
  },
  "meta": {
    "elapsed_ms": 1
  }
}
```

## Compatibility Notes

- This server emits canonical `ok/data/meta` and `ok/error/meta` envelopes.
