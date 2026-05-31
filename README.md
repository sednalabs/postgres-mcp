# postgres-mcp

`postgres-mcp` is a Rust stdio Model Context Protocol server for PostgreSQL.
It is designed for low-latency agent workflows, practical compatibility with
the Python `postgres-mcp` tool surface, and safe defaults around database
connectivity and SQL execution.

The default startup path is lazy: the process can initialize and advertise MCP
tools without requiring immediate database network I/O. Database connectivity is
validated on first use unless a stricter startup mode is configured.

## Highlights

- Stdio transport only; no HTTP listener is exposed.
- Lazy DB startup by default for spawn-per-request clients.
- Schema discovery, read-query, render, export, health, and advisor tools.
- Compatibility SQL surfaces remain available where needed.
- Mutating SQL is opt-in through `admin_sql` and explicit configuration.
- `execute_sql` is hidden from discovery by default to reduce accidental use.
- TLS and `sslmode` parsing fail closed for ambiguous or downgrade-prone input.
- Redaction-first telemetry avoids leaking raw database URIs, passwords, or SQL
  payloads.

## Quick Start

Install Rust, make sure any companion toolkit crates required by `Cargo.toml`
are available at their configured local paths, then build:

```bash
cargo build
```

Run over stdio with a local development database:

```bash
DATABASE_URI='postgresql://user:pass@localhost:5432/app?sslmode=disable' \
  cargo run -- --startup-db-connect=background
```

For verified TLS, prefer `sslmode=verify-full`:

```bash
DATABASE_URI='postgresql://user:pass@db.example.com:5432/app?sslmode=verify-full' \
  cargo run -- --startup-db-connect=background
```

See [Getting Started](docs/GETTING_STARTED.md) for client configuration,
startup modes, and smoke checks.

## Documentation

- [Getting Started](docs/GETTING_STARTED.md): local setup, stdio launch, and
  first smoke checks.
- [Tool Guide](docs/TOOL_GUIDE.md): public tool groups, recommended read paths,
  compatibility surfaces, and examples.
- [Security Model](docs/SECURITY_MODEL.md): SQL safety, credential handling,
  TLS, telemetry, and extension boundaries.
- [Build Helper MCP](docs/build_helper_mcp.md): optional preset-based
  validation for shared or remote runner environments.
- [Release Checklist](docs/release-checklist.md): public release validation and
  publication hygiene.
- [Migration and Rollout Guide](docs/migration-rollout.md): guidance for moving
  from Python `postgres-mcp` to this Rust stdio server.
- [Compatibility Lifecycle](docs/compatibility-lifecycle.md): policy for
  compatibility aliases and deprecations.
- [Payload v2 Contract](docs/payload-v2-contract.md): legacy compatibility
  envelope details for `execute_sql`.
- [Dependency Governance](docs/dependency-governance.md): dependency review and
  validation expectations.
- [Negative Validation Pack](docs/negative-validation-pack.md): resilience
  checks for malformed inputs, retry pressure, and backpressure behavior.
- [Security Policy](SECURITY.md): vulnerability reporting.
- [Contributing](CONTRIBUTING.md): development and documentation workflow.

## Tool Surface

Schema and metadata:

- `list_schemas`
- `list_objects`
- `get_object_details`

Structured read paths:

- `query_sql`
- `query_tuples`
- `render_sql`
- `export_sql`
- `describe_sql`

Session and async compatibility:

- `session_open`
- `session_status`
- `session_close`
- `query_start`
- `query_start_and_wait`
- `query_status`
- `query_cancel`

Administrative and compatibility surfaces:

- `admin_sql` when explicitly enabled.
- `execute_sql` when explicitly exposed.
- `query_job_start`, `export_job_start`, `job_status`, and `job_cancel` as
  deprecated compatibility tools.

Advisor and health tools:

- `explain_query`
- `get_top_queries`
- `analyze_db_health`
- `analyze_query_indexes`
- `analyze_workload_indexes`

Prefer `query_sql`, `query_tuples`, `render_sql`, and `export_sql` for new
agent-facing read workflows. Reserve `execute_sql` for compatibility or
advanced payload controls.

## Configuration

Database connection:

- `DATABASE_URI`
- Positional database URI argument

Access mode:

- `--access-mode restricted` is the default read-oriented mode.
- `--access-mode unrestricted` allows unrestricted SQL execution for explicitly
  enabled administrative paths.
- `--enable-admin-sql` or `POSTGRES_MCP_ENABLE_ADMIN_SQL=1` exposes
  `admin_sql`.
- `--expose-execute-sql` or `POSTGRES_MCP_EXPOSE_EXECUTE_SQL=1` exposes
  `execute_sql` in discovery.

Startup DB connection mode:

- `--startup-db-connect background` is the default lazy mode.
- `--startup-db-connect warn` probes on startup and continues on failure.
- `--startup-db-connect fail-fast` exits when the startup DB probe fails.

TLS behavior:

- `sslmode=disable` uses plain TCP.
- `sslmode=require` is rejected unless `--allow-insecure-tls` or
  `POSTGRES_MCP_ALLOW_INSECURE_TLS=1` is set.
- `sslmode=verify-ca` and `sslmode=verify-full` use native trust roots plus
  optional `sslrootcert=<path>`.
- `sslmode=prefer`, duplicate `sslmode`, and duplicate `sslrootcert` values are
  rejected.

Metadata policy:

- `--metadata-policy-mode full|limited|denied`
- `--metadata-schema-allow <schema>` repeatable
- `--metadata-schema-deny <schema>` repeatable

See [Security Model](docs/SECURITY_MODEL.md) for the safety posture behind
these defaults.

## Validation

For behavior changes, use the smallest validation that proves the change. When a
Build Helper MCP runner is available, the documented presets provide an
auditable execution surface:

```text
postgres-mcp.test
postgres-mcp.build
postgres-mcp.deferred-tool-discovery-smoke
postgres-mcp.integration-matrix-check
```

Local fallback commands are documented in [Build Helper MCP](docs/build_helper_mcp.md)
and [Release Checklist](docs/release-checklist.md). Documentation-only changes
can usually be validated with:

```bash
git diff --check
```

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE).
