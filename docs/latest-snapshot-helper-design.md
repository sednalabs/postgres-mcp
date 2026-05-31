# Latest-Snapshot Helper Design

**Status:** Draft design scope
**Date:** 2026-02-19  

## 1. Goal

Analysts repeatedly write equivalent "latest snapshot window" CTEs to pick the
latest row per key. This note is limited to designing a helper with:

- lower cognitive overhead than hand-written windowed CTEs,
- predictable SQL semantics,
- explicit parser and safety boundaries,
- backwards-compatible behavior for existing `execute_sql` callers.

## 2. User Problem (Current Pattern)

Teams often re-run variants of the same pattern:

```sql
WITH latest AS (
  SELECT
    tenant_id,
    MAX(snapshot_ts) AS snapshot_ts
  FROM events_snapshot
  WHERE deleted_at IS NULL
  GROUP BY tenant_id
),
ranked AS (
  SELECT
    s.*
  FROM events_snapshot s
  JOIN latest l
    ON l.tenant_id = s.tenant_id
   AND l.snapshot_ts = s.snapshot_ts
  WHERE s.deleted_at IS NULL
)
SELECT *
FROM ranked;
```

The helper should remove this repetition while keeping query semantics
transparent to analysts and operators.

## 3. UX Options

### Option 1 (Recommended): SQL relation macro `latest_snapshot(...)`

Add a reserved helper invocation inside `FROM`:

```sql
SELECT *
FROM latest_snapshot(
  source => 'public.events_snapshot',
  ts_column => 'snapshot_ts',
  partition_by => ARRAY['tenant_id'],
  tie_breakers => ARRAY['id'],
  include_null_timestamps => false
) AS snap
WHERE snap.status = 'ready'
```

This expands to a strict, deterministic, one-row-per-partition relation.

### Option 2: Top-level comment directive

Directive at query head, e.g.:

```sql
/* latest_snapshot: table=public.events_snapshot ts=snapshot_ts partition_by=tenant_id */
SELECT * FROM public.events_snapshot ...
```

This avoids new SQL surface syntax but creates implicit query rewrite rules that are
less obvious to users and harder to reason about with nested statements.

### Option 3: Dedicated `execute_sql` argument

Introduce a new `helpers` argument to `execute_sql` and keep raw SQL unchanged.
This has a cleaner API boundary but is less discoverable for direct SQL users and
does not generalize naturally to ad-hoc SQL snippets shared across tools.

## 4. Recommended Decision

Adopt **Option 1**.

- It is explicit in SQL text.
- It fits typical analyst workflows (inline helper in `FROM`).
- It provides the clearest place for parser impact analysis.

## 5. Helper Contract

### Invocation

`latest_snapshot(...)` in any relation position (typically under `FROM`).

### Required arguments

1. `source` — quoted source relation name (schema-qualified table/view name).
2. `ts_column` — timestamp-like column name used for freshness.

### Optional arguments

1. `partition_by` — `ARRAY[...]` of identifiers.
2. `tie_breakers` — `ARRAY[...]` identifiers for deterministic tiebreaking
   within identical timestamps.
3. `include_null_timestamps` — `boolean` (default `false`).
4. `nulls_first` — `boolean` (default `false` if timestamps are descending,
   equivalent to `NULLS LAST`; explicit for clarity).
5. `as_of` — optional SQL literal or parameter placeholder to support replay windows
   (deferred to later phase; not required for initial delivery).

### Expansion result

Helper expands to a relation that yields exactly one row per partition, where:

- The row ordering for ranking is:
  `ts_column DESC`, then `tie_breakers...`, then all source primary-key columns if
  discoverable, then stable fallback tuple order.
- Partition cardinality is:
  one partition per distinct `partition_by` combination.
- If `partition_by` is empty, helper returns the latest row globally.

### Duplicate timestamp policy

For partitions where multiple rows share the same max timestamp:

- rows are ordered by `tie_breakers` and then deterministic fallback,
- only the top-ranked row is returned.

### Null timestamp policy

- Default excludes `NULL` timestamps from consideration (`include_null_timestamps = false`).
- If set true, null timestamps are treated as last/first according to `nulls_first`
  after non-null values are ranked by `ts_column` direction.
- A full-null partition returns no rows when `include_null_timestamps = false`;
  returns one row when true and tie-breakers select a single deterministic row.

### Partition key edge cases

- `partition_by` values that are nullable are allowed and participate in grouping.
- Empty `partition_by` yields global latest semantics.
- Very large partition lists are rejected by policy limits.

## 6. Parser Impact

Because this is a preprocessor helper (not a PostgreSQL-native function), parser
work is needed in `execute_sql` execution path before DB submission.

### Minimal parser requirements

1. Recognize helper invocations outside of:
   - single-quoted strings,
   - double-quoted identifiers,
   - dollar-quoted bodies,
   - line/block comments,
   - semicolon-separated later statements.
2. Parse argument list with a small state machine:
   - token boundaries,
   - quoted identifiers/strings,
   - nested parentheses.
3. Validate helper arity and argument keys before expansion.
4. Emit a stable error if parsing/expansion fails (do not silently execute
   unexpanded SQL).

### Execution order

1. Parse incoming SQL statement(s).
2. Expand helper invocations to rewritten SQL.
3. Run safety checks (`classify_restricted_sql`) on rewritten SQL to avoid
   macro-based bypasses.
4. Preserve original SQL text in debug metadata (`meta.expanded_sql`) for
   transparency.

## 7. Safety and SQL Transparency

- Preserve original SQL text.
- Include expanded SQL in tool metadata so operators can inspect generated SQL.
- Use explicit allow-lists:
  helper `source` must be qualified identifier text (no semicolons, no statements).
- Do not allow helper to invoke arbitrary expressions for `source`; keep scope to
  table/view identifiers plus an optional SQL `WHERE` predicate if later design
  phase adds it.
- Expand only in `execute_sql` and reuse existing restricted-mode guardrails after
  expansion.

## 8. Backward Compatibility

- Existing queries without `latest_snapshot(...)` are unchanged.
- Identifiers or function names `latest_snapshot` in user SQL outside supported helper
  grammar keep executing unchanged unless explicit helper syntax is recognized.
- Add a reserved-token strategy (`latest_snapshot` keyword is reserved only in
  `FROM`-like helper position for this version).
- If expansion fails, request returns a deterministic SQL policy/validation error.

## 9. Open Questions

1. Whether to allow `source` as a subquery reference in v1 (likely no; table/view
   only).
2. Whether to expose `as_of` in v1 or keep deterministic latest as of now only.
3. Whether to include helper diagnostics in `meta.query_hints` or dedicated `meta
   .helpers` section.

## 10. Implementation Note (Out of Scope for this WI)

This note is design-only. The next implementation pass should add a parser-safe expansion
pass, add parser-focused unit tests, and add integration coverage for:

- global latest,
- partitioned latest,
- ties with deterministic tie-breakers,
- null timestamp partitions with and without `include_null_timestamps`.
