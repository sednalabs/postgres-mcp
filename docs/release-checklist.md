# Release Checklist

Use this checklist before publishing a release, opening a public release PR, or
promoting a build for downstream MCP clients.

## References

- [Getting Started](GETTING_STARTED.md)
- [Tool Guide](TOOL_GUIDE.md)
- [Security Model](SECURITY_MODEL.md)
- [Build Helper MCP](build_helper_mcp.md)
- [Safety Checklist](SAFETY_CHECKLIST.md)
- [Migration and Rollout Guide](migration-rollout.md)
- [Compatibility Lifecycle](compatibility-lifecycle.md)
- [Dependency Governance](dependency-governance.md)
- [Negative Validation Pack](negative-validation-pack.md)
- [Reliability Exercises](reliability-exercises.md)

## Build and Tests

Preferred Build Helper MCP presets when available:

1. `postgres-mcp.test`
2. `postgres-mcp.build`
3. `postgres-mcp.build-release`
4. `postgres-mcp.deferred-tool-discovery-smoke`
5. `postgres-mcp.dependency-governance-check`

Local fallback:

```bash
cargo test
cargo build
cargo build --release
./scripts/deferred_tool_discovery_smoke.sh --startup-db-connect=background
./scripts/dependency_governance_check.sh
```

## Contract and Safety

Run the checks that match the release scope:

```bash
./scripts/contract_parity_check.sh
./scripts/parity_manifest_check.sh
./scripts/sql_policy_contract_rebaseline.sh
./scripts/sql_policy_toolkit_conformance.sh
./scripts/sql_policy_conformance_diff.sh
./scripts/runtime_safety_conformance.sh --require-db-runtime
./scripts/index_advisor_repro_check.sh
./scripts/run_canary_parity.sh
```

For performance-sensitive releases:

```bash
./scripts/cold_start_bench.sh
```

Confirm `.tmp/perf/perf_gate_report.json` reports passing enforced scenarios.
DB-backed performance scenarios require `DATABASE_URI` unless intentionally run
with DB scenarios disabled.

## Manual Smoke

1. Start the server with placeholder-safe local credentials.
2. Run an MCP handshake through a probe client.
3. Confirm discovery returns core schema and read tools.
4. Run one schema discovery call such as `list_schemas`.
5. Run one read query through `query_sql` or `render_sql`.
6. Confirm `admin_sql` and `execute_sql` remain hidden unless explicitly
   enabled for the test.

## Publication Hygiene

Before publishing:

1. Scan docs, workflow summaries, release notes, branch names, commit messages,
   and PR text for credentials, hostnames, non-public repository names,
   non-public release names, and deployment-specific identifiers.
2. Confirm examples use placeholders or local development values.
3. Confirm public docs do not depend on non-public policy names or restricted
   infrastructure details.
4. Confirm dependency changes include a public dependency note as described in
   [Dependency Governance](dependency-governance.md).
5. Confirm tool-schema snapshot changes are intentional and explained.

## Rollout Notes

- Default startup mode is `background`.
- For managed service wrappers that must prove DB readiness, use
  `--startup-db-connect fail-fast`.
- Keep `admin_sql` disabled unless mutating SQL is explicitly required.
- Keep `execute_sql` hidden from discovery unless compatibility clients need it.
- Prefer verified TLS for remote databases.
- Keep rollback notes tied to exact commands, artifacts, and stable error codes.
