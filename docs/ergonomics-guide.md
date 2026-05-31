# Postgres MCP Ergonomics Guide

This guide collects practical SQL patterns for common Postgres MCP query pitfalls and
reusable templates. It is optimized for quick copy/paste during analysis sessions.

## 1) Numeric formatting and casting

When converting stored integer cents values, cast before math to avoid integer truncation.

### Convert cents to major units with fixed precision

```sql
SELECT
  price_cents::numeric / 100 AS price_dollars,
  COALESCE(discount_cents::numeric, 0) / 100 AS discount_dollars
FROM public.orders;
```

### Round consistently (avoid accidental integer truncation)

```sql
SELECT
  ROUND(total_cents::numeric / 100, 2) AS total_dollars_rounded,
  ROUND((gross_cents - fee_cents)::numeric / 100, 2) AS net_dollars
FROM public.settlements;
```

### Aggregate money safely

Aggregate in cents first, then divide once at the end.

```sql
SELECT
  tenant_id,
  SUM(amount_cents)::numeric / 100 AS total_dollars
FROM public.transactions
GROUP BY tenant_id;
```

If you need presentation-only decimal text, keep raw integer cents in the result and
add a companion formatted column (`_cents_formatted`) in your consuming app.

## 2) FILTER syntax and common syntax errors

`FILTER` is valid only as `aggregate_function(args) FILTER (WHERE condition)`.

```sql
SELECT
  COUNT(*) FILTER (WHERE status = 'ready') AS ready_rows,
  COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL '24 hours') AS recent_rows
FROM public.review_queue;
```

Invalid pattern to avoid:

```sql
-- This throws a syntax error (e.g. SQLSTATE 42601)
SELECT
  COUNT(*) FILTER (created_at >= NOW()) AS bad_rows
FROM public.review_queue;
```

Alternative when teams prefer branch-style predicates:

```sql
SELECT
  COUNT(CASE WHEN status = 'ready' THEN 1 END) AS ready_rows
FROM public.review_queue;
```

## 3) Alias scope pitfalls and safer rewrites

Use this rule of thumb:
- Select-list aliases are not available in `WHERE`.
- Table aliases must be unique per scope.
- Resolve ambiguity by repeating expressions or using a subquery/CTE boundary.

### Alias used too early in the same SELECT

```sql
-- Alias is not yet visible in WHERE
SELECT
  event_ts - INTERVAL '1 day' AS event_day
FROM public.events
WHERE event_day >= NOW() - INTERVAL '7 days';
```

Use a CTE when alias reuse is needed:

```sql
WITH normalized AS (
  SELECT event_ts - INTERVAL '1 day' AS event_day
  FROM public.events
)
SELECT *
FROM normalized
WHERE event_day >= NOW() - INTERVAL '7 days';
```

### Alias collision across join inputs

```sql
SELECT *
FROM public.accounts AS a
JOIN public.accounts AS a ON a.owner_id = a.owner_id;
```

Fix by using distinct aliases:

```sql
SELECT *
FROM public.accounts AS parent
JOIN public.accounts AS child
  ON child.parent_id = parent.id;
```

### Missing relation/column alias errors at scan time

When you see errors like:
- `missing FROM-clause entry for table "x"`
- `column "foo" does not exist` (`42703`)

verify that every identifier resolves to the correct alias and scope before re-running.

### One-call discovery before retrying

Use `list_objects` to discover candidate relations and columns in one round-trip:

```json
{
  "schema_name": "public",
  "object_type": "table",
  "name_like": "coverage",
  "include_columns": true
}
```

Tips:
- For relation errors (`42P01`), start with `name_like` set to the missing relation token.
- For alias-scope errors (`missing FROM-clause entry`), verify each JOIN alias maps to a relation in the `FROM`/`JOIN` chain.
- For missing-column errors (`42703`), inspect the returned `columns` arrays before editing projections or predicates.

## 4) Canonical latest-snapshot query patterns

Latest row per partition is a common reporting pattern. Prefer deterministic CTE-based templates.

### Global latest row per key

```sql
SELECT s.*
FROM public.events_snapshot AS s
JOIN (
  SELECT MAX(snapshot_ts) AS latest_snapshot_ts
  FROM public.events_snapshot
) m
  ON s.snapshot_ts = m.latest_snapshot_ts;
```

### Latest row per tenant (partitioned)

```sql
WITH ranked AS (
  SELECT
    s.*,
    ROW_NUMBER() OVER (
      PARTITION BY tenant_id
      ORDER BY snapshot_ts DESC, id DESC
    ) AS rn
  FROM public.events_snapshot AS s
  WHERE s.tenant_id IS NOT NULL
)
SELECT *
FROM ranked
WHERE rn = 1
ORDER BY tenant_id;
```

### Latest row per tenant excluding null timestamps

```sql
WITH ranked AS (
  SELECT
    s.*,
    ROW_NUMBER() OVER (
      PARTITION BY tenant_id
      ORDER BY snapshot_ts DESC, id DESC
    ) AS rn
  FROM public.events_snapshot AS s
  WHERE s.snapshot_ts IS NOT NULL
    AND s.tenant_id IS NOT NULL
)
SELECT *
FROM ranked
WHERE rn = 1;
```

### Recommended SQL helper form

When enabled in this MCP branch, `latest_snapshot(...)` can render the same template in-place:

```sql
SELECT *
FROM latest_snapshot(
  source => 'public.events_snapshot',
  ts_column => 'snapshot_ts',
  partition_by => ARRAY['tenant_id']
) AS latest;
```

If helper-specific details are needed, include a short `query_hints` note or comment
in your request so peers can reuse the same shape across incidents.
