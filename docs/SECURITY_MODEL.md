# Security Model

`postgres-mcp` treats database connectivity and SQL execution as high-risk
surfaces. The default configuration is designed to keep read workflows easy
while requiring explicit opt-in for mutating or compatibility-heavy paths.

## Transport Boundary

The server uses stdio only. It does not expose an HTTP listener, bind a TCP
port, or implement a network authentication layer. Authentication to PostgreSQL
is handled by the database connection string and PostgreSQL itself.

## Credential Handling

- Provide database credentials through `DATABASE_URI`, a positional URI
  argument, or a service wrapper that injects environment variables.
- Do not commit real database URIs, passwords, certificates, or service account
  material.
- Error messages, telemetry, and diagnostic snapshots must report whether a
  database URI is configured without printing the URI itself.
- Use placeholders such as `user:pass@localhost` in public examples.

## TLS and DSN Parsing

The DSN parser rejects ambiguous or downgrade-prone TLS configuration:

- duplicate `sslmode` values are rejected
- duplicate `sslrootcert` values are rejected
- `sslmode=prefer` is rejected
- `sslmode=require` is rejected unless insecure TLS is explicitly allowed

Prefer `sslmode=verify-full` for remote databases. Use `sslmode=disable` only
for local development or trusted local networks.

## SQL Access Modes

Restricted mode is the default:

- read-oriented tools remain available
- mutating SQL is denied by the restricted execution path
- `admin_sql` is hidden unless explicitly enabled
- `execute_sql` is hidden from discovery unless explicitly exposed

Administrative SQL requires both an intentional access posture and explicit
tool exposure:

```bash
POSTGRES_MCP_ENABLE_ADMIN_SQL=1 \
  postgres-mcp --access-mode unrestricted
```

Expose `execute_sql` only for compatibility flows that need its legacy payload
controls:

```bash
POSTGRES_MCP_EXPOSE_EXECUTE_SQL=1 postgres-mcp
```

For new read workflows, prefer `query_sql`, `query_tuples`, `render_sql`, and
`export_sql`.

## Query Budgets

The server applies bounded query controls by default:

- DB query timeout
- PostgreSQL statement timeout
- PostgreSQL lock timeout
- response page limits
- optional cell clipping
- optional export-to-file pagination controls

These controls limit accidental large payloads and long-running statements, but
they are not a substitute for database-side permissions, row-level security, or
dedicated read-only roles.

## Metadata Policy

Metadata discovery can be constrained:

```bash
--metadata-policy-mode full|limited|denied
--metadata-schema-allow <schema>
--metadata-schema-deny <schema>
```

Use `limited` for environments where only selected schemas should be visible to
agent clients. Use `denied` when schema discovery must be unavailable.

## Extension Boundary

Advisor tools are provider-neutral by default. External advisor mode is
disabled unless configured with an explicit command, timeout, attempt limit, and
fallback policy.

Provider-specific integrations should live outside this repository and connect
through the documented external advisor contract. Keep public docs and examples
free of deployment-specific provider identifiers.

## Telemetry and Errors

Telemetry is redaction-first:

- low-cardinality error fields are emitted by default
- raw SQL and raw database error text are avoided in telemetry
- debug previews are opt-in and clipped
- response errors include deterministic codes, reasons, fingerprints, and
  retryability where applicable

When filing public issues, include tool name, stable code, reason, and a
redacted reproduction. Do not include hostnames, raw URIs, passwords,
non-public schema names, or production query text.

## Validation Expectations

Security-sensitive changes require automated validation when a reasonable test
seam exists. At minimum, run the relevant runtime safety, SQL policy, and tool
schema checks documented in [Release Checklist](release-checklist.md).
