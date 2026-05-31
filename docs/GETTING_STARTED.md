# Getting Started

This guide gets `postgres-mcp` running as a stdio MCP server with safe local
defaults.

## Prerequisites

- Rust toolchain compatible with the repository edition.
- A PostgreSQL database for DB-backed smoke checks.
- Any companion toolkit crates referenced by `Cargo.toml` at their configured
  local paths, unless your package source replaces those path dependencies.

For a local development database, any disposable PostgreSQL instance is enough.
Use placeholder credentials in examples and keep real credentials in local
environment variables or secret managers.

## Build

```bash
cargo build
```

For a release binary:

```bash
cargo build --release
```

## Run Over Stdio

The server communicates over stdin/stdout. It does not open an HTTP port.

```bash
DATABASE_URI='postgresql://user:pass@localhost:5432/app?sslmode=disable' \
  cargo run -- --startup-db-connect=background
```

For TLS-verified remote connections:

```bash
DATABASE_URI='postgresql://user:pass@db.example.com:5432/app?sslmode=verify-full' \
  ./target/debug/postgres-mcp --startup-db-connect=background
```

Generic MCP client command shape:

```toml
[mcp_servers.postgres]
command = "/absolute/path/to/postgres-mcp"
args = ["--startup-db-connect=background"]

[mcp_servers.postgres.env]
DATABASE_URI = "postgresql://user:pass@localhost:5432/app?sslmode=disable"
```

Use the promoted-artifact launcher when your environment publishes local build
metadata and you want client restarts to pick up the newest already-built binary:

```toml
[mcp_servers.postgres]
command = "/absolute/path/to/scripts/launch_postgres_mcp_from_promoted.sh"
args = ["--startup-db-connect=background"]
```

The launcher never builds. It fails closed when no promoted binary is available.

## Startup DB Modes

- `background` starts the MCP server without blocking on DB reachability.
- `warn` probes the DB during startup and continues on failure.
- `fail-fast` probes the DB during startup and exits on failure.

Use `background` for agent clients that frequently spawn the server. Use
`fail-fast` for managed service wrappers that should not become ready unless
the database is reachable.

## First Smoke Checks

Print the current tool inventory:

```bash
./target/debug/postgres-mcp --print-tools
```

Validate deferred tool discovery after a build:

```bash
./scripts/deferred_tool_discovery_smoke.sh --startup-db-connect=background
```

Run a schema discovery call from your MCP client:

```json
{
  "schema_name": "public",
  "object_type": "table",
  "include_columns": true,
  "limit": 20
}
```

Then run a read query through `query_sql`:

```json
{
  "sql": "select id, created_at from public.orders order by created_at desc limit 10"
}
```

## Optional Build Helper

If your environment provides a Build Helper MCP runner, prefer the repository
presets for repeatable validation:

```text
postgres-mcp.test
postgres-mcp.build
postgres-mcp.deferred-tool-discovery-smoke
postgres-mcp.integration-matrix-check
```

See [Build Helper MCP](build_helper_mcp.md) for the full preset list and local
fallback commands.
