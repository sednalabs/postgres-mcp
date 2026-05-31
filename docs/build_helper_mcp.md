# Build Helper MCP

This repository can run validation through an optional Build Helper MCP runner.
The presets are useful on shared, remote, or resource-constrained hosts because
they provide repeatable commands and an auditable execution trail.

The runner is optional. When it is unavailable, use the local fallback commands
listed below.

## Preset Source

- `.build-helper/presets.json`

## Preset IDs

- `postgres-mcp.build`
- `postgres-mcp.build-release`
- `postgres-mcp.test`
- `postgres-mcp.deferred-tool-discovery-smoke`
- `postgres-mcp.test-update-tool-snapshots`
- `postgres-mcp.contract-parity-check`
- `postgres-mcp.parity-manifest-check`
- `postgres-mcp.sql-policy-toolkit-conformance`
- `postgres-mcp.sql-policy-conformance-diff`
- `postgres-mcp.runtime-safety-conformance`
- `postgres-mcp.parity-semantic-diff`
- `postgres-mcp.index-advisor-repro-check`
- `postgres-mcp.integration-matrix-check`
- `postgres-mcp.dependency-governance-check`
- `postgres-mcp.ergonomics-rollout-validation`

## Typical Validation Order

For a normal behavior change:

1. `postgres-mcp.test`
2. `postgres-mcp.build`
3. `postgres-mcp.deferred-tool-discovery-smoke`

For release promotion:

1. `postgres-mcp.test`
2. `postgres-mcp.build-release`
3. `postgres-mcp.contract-parity-check`
4. `postgres-mcp.parity-manifest-check`
5. `postgres-mcp.runtime-safety-conformance`
6. `postgres-mcp.integration-matrix-check`
7. `postgres-mcp.dependency-governance-check`

Use `postgres-mcp.test-update-tool-snapshots` only when an intentional tool
schema rebaseline is part of the change.

## Local Fallbacks

```bash
cargo test
cargo build
cargo build --release
./scripts/deferred_tool_discovery_smoke.sh --startup-db-connect=background
./scripts/contract_parity_check.sh
./scripts/parity_manifest_check.sh
./scripts/runtime_safety_conformance.sh
./scripts/run_canary_parity.sh
./scripts/dependency_governance_check.sh
```

Some checks require optional runtime dependencies such as PostgreSQL, Docker, or
an external probe binary. If a prerequisite is missing, capture the failure and
rerun once the prerequisite is available.

## Deferred Tool Discovery

The stdio server should initialize and expose a non-empty tool inventory without
needing an eager DB connection:

```bash
./scripts/deferred_tool_discovery_smoke.sh --startup-db-connect=background
```

The smoke fails when initialization fails, tool discovery is empty, core tools
are missing, or `execute_sql` is exposed without an explicit exposure request.

## Promoted Binary Launcher

MCP clients may point at:

```bash
scripts/launch_postgres_mcp_from_promoted.sh
```

The launcher never builds. It executes the newest already-built promoted binary
and fails closed when no promoted binary exists.
