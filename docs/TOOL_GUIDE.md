# Tool Guide

This guide describes the public tool surface by workflow. Tool names are
contract-sensitive; changing them requires an intentional compatibility plan.

## Schema Discovery

Use schema discovery before writing SQL against unfamiliar databases.

- `list_schemas`: list visible schemas.
- `list_objects`: list tables, views, and other objects with optional filters.
- `get_object_details`: inspect a selected object.

Example `list_objects` request:

```json
{
  "schema_name": "public",
  "object_type": "table",
  "name_contains": "order",
  "include_columns": true,
  "limit": 20
}
```

Only one name filter may be provided at a time:

- `name_exact`
- `name_prefix`
- `name_contains`
- `name_pattern`
- `name_like` for legacy compatibility

## Preferred Read Tools

Use these tools for new read workflows:

- `query_sql`: returns JSON row objects.
- `query_tuples`: returns ordered columns plus tuple rows.
- `render_sql`: returns markdown text for reader-first inspection.
- `export_sql`: writes result payloads to MCP resources.
- `describe_sql`: prepares and describes result columns without executing the
  query body.

Example `query_sql` request:

```json
{
  "sql": "select id, status, created_at from public.orders order by created_at desc limit 10"
}
```

Example `query_tuples` request:

```json
{
  "sql": "select status, count(*) from public.orders group by status order by status"
}
```

Example `render_sql` request:

```json
{
  "sql": "select id, email from public.customers order by id limit 5",
  "profile": "compact"
}
```

Example `export_sql` request:

```json
{
  "sql": "select id, total_cents, created_at from public.orders order by id",
  "format": "csv"
}
```

## Sessions

Pinned sessions are useful when a workflow needs temporary state or repeated
reads against the same PostgreSQL session.

1. Open with `session_open`.
2. Pass `session_id` to `query_sql`, `query_tuples`, `render_sql`, or
   compatibility `execute_sql`.
3. Close with `session_close`.

Idle sessions expire automatically after the configured timeout.

## Async Reads and Exports

Preferred async execution uses task-augmented structured tools:

- call `query_sql`, `query_tuples`, or `export_sql` with task options
- poll with `tasks/get`
- fetch final payloads with `tasks/result`
- cancel with `tasks/cancel`

Compatibility async controls remain available:

- `query_start`
- `query_start_and_wait`
- `query_status`
- `query_cancel`
- `query_job_start`
- `export_job_start`
- `job_status`
- `job_cancel`

The `query_job_start`, `export_job_start`, `job_status`, and `job_cancel`
surfaces are deprecated compatibility tools.

## Compatibility SQL Surface

`execute_sql` remains available for advanced compatibility flows such as:

- legacy v2 `ok/data/meta` envelopes
- detailed pagination metadata
- explicit output mode controls
- count-mode controls
- metadata verbosity controls
- export-to-file compatibility

It is hidden from discovery by default. Expose it only when a client depends on
that surface:

```bash
POSTGRES_MCP_EXPOSE_EXECUTE_SQL=1 postgres-mcp
```

For most reads, prefer `query_sql`, `query_tuples`, or `render_sql`.

## Administrative SQL

`admin_sql` is disabled and hidden by default. Enable it only for controlled
maintenance workflows:

```bash
POSTGRES_MCP_ENABLE_ADMIN_SQL=1 \
  postgres-mcp --access-mode unrestricted
```

Large `RETURNING` payloads are rejected instead of silently truncated.

## Health and Advisor Tools

Operational tools:

- `get_top_queries`
- `analyze_db_health`
- `explain_query`

Advisor tools:

- `analyze_query_indexes`
- `analyze_workload_indexes`

Advisor tools default to deterministic local analysis. Optional external
advisor mode is disabled unless configured explicitly.

## Safe Query Patterns

Use explicit projections and limits for quick agent loops:

```json
{
  "sql": "select id, status, created_at from public.orders order by created_at desc limit 25",
  "max_rows": 25
}
```

Use bound parameters for user-provided values:

```json
{
  "sql": "select id, status from public.orders where customer_id = $1 order by id limit 20",
  "params": [42]
}
```

Use typed wrappers for ambiguous nulls or empty arrays:

```json
{
  "sql": "select id from public.orders where cancelled_at is not distinct from $1",
  "params": [
    { "type": "text", "value": null }
  ]
}
```

Avoid unbounded `select *` on large relations. Use schema discovery first when a
query fails with missing relation or missing column errors.
