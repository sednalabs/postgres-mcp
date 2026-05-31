# Negative Validation Pack

This document defines deterministic negative and resilience validation lanes for
`postgres-mcp`.

## Scope

The pack targets four behavior classes:

1. malformed startup dependency tokens
2. parser acceptance and rejection invariants for dependency inputs
3. retry contention against per-tool circuit-breaker state
4. bounded backpressure guidance and open-circuit behavior

## Test Lanes

Smoke lane:

```bash
cargo test --locked startup_dependency
cargo test --locked circuit_breaker
```

Extended lane:

```bash
cargo test --locked retry_storm -- --ignored
```

If a focused filter does not match the current source, inspect the available
tests and run the equivalent resilience checks directly:

```bash
cargo test -- --list
```

## Determinism Expectations

- Smoke tests should avoid external DB dependencies.
- Extended concurrency tests should keep deterministic pass/fail outcomes while
  allowing normal timing variance.
- Error and backpressure envelopes must remain low-cardinality and
  contract-safe.
