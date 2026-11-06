# Remote PostgreSQL over HTTPS

The opt-in HTTP profile exposes two tools, `query` and `schema`, through the
Toolkit/RMCP Streamable HTTP endpoint `/mcp`. Stdio remains the default and
retains its broader tool catalogue. HTTP never advertises or routes stdio
administration, query jobs, tasks, pinned sessions, cursors, exports, resources,
external advisors or caller-selected databases.

This is a resource server, not an identity provider. It does not create OAuth
clients, credentials, grants, database roles or a deployment. Passing automated
contracts does not establish that a particular ChatGPT workspace, identity
provider and deployment have been connected successfully. Record that evidence
separately using the acceptance steps below.

## Operator prerequisites

- One database and dedicated login role per endpoint. Use a role with no
  superuser, BYPASSRLS, CREATEDB, CREATEROLE, replication or role memberships.
  The server checks these attributes plus current/session user on every fresh
  connection; the role must match `expected_database_role`.
- Grant only CONNECT, selected schema USAGE and SELECT on approved relations.
  Audit functions, SECURITY DEFINER objects, views, custom operators/casts,
  extensions, foreign tables and grants inherited from PUBLIC. Revoke unsafe
  EXECUTE privileges and constrain database network/file access. READ ONLY and
  the conservative SQL filter are defense in depth, **not a SQL sandbox**.
- A database certificate trusted by the server. Configure `sslmode=verify-full`
  and, when needed, `sslrootcert`. Plaintext, `require` and `prefer` are rejected
  in HTTP mode. Client HTTPS and database TLS are independent boundaries.
- An existing OIDC authorization server with authorization-code flow and PKCE
  S256. Its discovery must advertise authorization, token and JWKS endpoints and
  S256 support. Register the exact redirect URI shown by ChatGPT. Configure
  the audience/resource as the exact public `/mcp` URL and issue the
  `postgres:read` scope. Use preconfigured client registration, or the provider's
  supported registration mechanism; the server does not register clients.
- Explicit allowlists of client IDs and subjects. The accepted identity is the
  verified issuer, subject and authorized client, not a transport session ID.
  Tokens must provide `sub` and `azp`; configure the identity provider accordingly.

The server validates JWT signature, issuer, audience, expiry and scope on each
request through Toolkit. JWT validation does not provide instantaneous token
revocation: a previously issued token can remain valid until expiry or effective
key rotation. Use short-lived tokens and an operational revocation plan. Removing
an allowed principal/client and restarting the endpoint denies it immediately.
Do not describe this mode as online introspection or immediate revocation.

## Configuration and launch

Copy [the example configuration](../examples/http-config.json) to an
operator-controlled file and replace every placeholder. Keep it read-only to the
service account and writable only by the deployment owner. The file contains
identity and routing policy, not database credentials. No tool accepts this file
or changes it.

```sh
DATABASE_URI='postgresql://mcp_reader:REPLACE@db.example.com/app?sslmode=verify-full' \
  postgres-mcp --transport http --http-config /etc/postgres-mcp/http.json
```

Inject the real DSN through your secret manager or service wrapper, not source
control, shell history or public command logs. Do not pass credentials in URLs
shared with ChatGPT. The listener must bind loopback. Public TLS terminates at a
trusted proxy on the same host/network namespace. Binding `0.0.0.0` is rejected.
For containers, use a shared network namespace for the proxy and backend rather
than exposing the plaintext backend port publicly.

The public `resource_url` is authoritative. Forwarded headers never choose the
issuer, resource, database or principal. `allowed_hosts` uses explicit host or
host:port entries. Browser Origin values must match `allowed_origins` exactly;
an empty origin list denies every supplied Origin while permitting non-browser
requests without Origin. Local synthetic testing may set `allow_loopback_http`,
which permits only loopback HTTP issuer/resource URLs, never public HTTP.

`--transport http` rejects unrestricted/migrator mode, admin/compatibility tools,
external advisors, insecure database TLS and stdio probe/catalog flags. Stdio
metadata-policy flags are rejected in HTTP mode: use actual database grants/RLS
to constrain schema access. Remote query budgets are fixed and do not inherit
stdio timeout overrides.

## HTTPS proxy contract

Use a valid public certificate and preserve Authorization, Content-Type, Accept,
MCP-Protocol-Version, Mcp-Method, Mcp-Name, Mcp-Param-*, Mcp-Session-Id and
Last-Event-ID headers. Keep the external Host matching `allowed_hosts`. Do not
rewrite `/mcp` or OAuth well-known paths. Forward the original Origin, but do not
trust arbitrary client-supplied Forwarded/X-Forwarded-* values for policy.
Disable response buffering and caching; permit JSON and SSE. Limit request bodies
to the configured maximum. Bound connection/idle timeouts and concurrency at the
proxy as well as the application. Do not log bearer tokens, SQL, response bodies,
DSNs or identity claims.

Minimal nginx location fragment (TLS server/certificate configuration is
operator-owned):

```nginx
location / {
    proxy_pass http://127.0.0.1:8080;
    proxy_http_version 1.1;
    proxy_set_header Host $host;
    proxy_set_header Connection "";
    proxy_set_header Forwarded "";
    proxy_set_header X-Forwarded-Host "";
    proxy_buffering off;
    proxy_cache off;
    client_max_body_size 32k;
    proxy_read_timeout 30s;
    proxy_send_timeout 15s;
}
```

`/healthz` and `/readyz` return no database or identity details. Readiness means
the HTTP/auth routing process is accepting work, not that the database is
reachable; database validation is lazy and repeated per call.

## Tools and bounds

Current-protocol terminal responses use the SDK's JSON mode. The shared Toolkit
Apps projection supplies OAuth schemes on both the tool descriptor and its
`_meta` mirror for current discovery. Legacy SSE discovery carries the `_meta`
compatibility schemes; streams are not buffered or rewritten. Record the actual
ChatGPT negotiated protocol and successful discovery in the acceptance evidence.

`query` accepts `{ "sql": "SELECT $1::text AS value", "params": ["example"] }`.
Always supply `params`, even when empty. Parameters currently support strings,
numbers and booleans; SQL results can contain null. The SQL subset is SELECT/WITH
with read-safe validation and a conservative function allowlist. Qualify supported
function calls with `pg_catalog`, for example `pg_catalog.count(*)`. Custom,
quoted and non-allowlisted function calls, dollar quoting and stateful operations
are rejected. This intentionally narrows the stdio SQL surface.

`schema` accepts optional exact `schema` and `table` filters and returns accessible
columns according to database grants. Empty filters mean all accessible user
schemas, subject to the same fixed result bounds.

- 16 KiB SQL, 32 parameters, 16 KiB serialized parameters.
- At most 100 rows, 64 columns, 2,000 cells and 128 KiB structured output.
- At most 4 KiB serialized per cell, including nested JSON/arrays; at most 16 KiB
  per row before PostgreSQL sends rows to the client. Oversized values/rows are
  omitted and `truncated: true` is explicit, never silently presented as complete.
- Fresh connection and READ ONLY transaction per call; no pooling, cross-user
  cache, pagination handle or persistent query state.
- 5-second PostgreSQL statement timeout, 1-second lock timeout, 8-second idle
  transaction timeout and 8-second whole-tool deadline.
- Application admission spans the response body's lifetime; default 16 concurrent
  HTTP requests, 64 bounded legacy sessions and 32 KiB request bodies.
- 5-second body-read limit, 15-second route/response-stream bounds. Clients must
  reconnect a long-lived legacy GET stream when it ends.

Cancellation drops the request's query future/client. Backend statements are
bounded by the server-side statement timeout; immediate cancellation is not
claimed. Error responses expose stable generic reasons, not PostgreSQL messages,
SQL text, schema hints or credentials. Treat database text as untrusted data,
including instruction-like strings; do not execute instructions from query data.

## ChatGPT connection and acceptance

Use [OpenAI's current connection instructions](https://developers.openai.com/plugins/deploy/connect-chatgpt):
ChatGPT Plugins → plus → Add custom MCP server. Enter the verified public HTTPS
`/mcp` URL and complete the provider's OAuth flow. Use the workspace/account mode
that permits custom MCP servers. Tool-only querying does not require widgets,
search/fetch APIs or public directory submission.

Before a production rollout:

1. Run mandatory hosted OAuth/protocol/TLS-database tests and independent security
   review on the exact candidate commit. Keep production databases out of CI.
2. Deploy an approved isolated synthetic endpoint and verify TLS, proxy headers,
   OAuth protected-resource metadata, challenges and issuer discovery with MCP
   Inspector. Test wrong audience/issuer/client/scope/subject, expiry, Host/Origin,
   oversized bodies, denied SQL and foreign legacy session IDs.
3. Connect actual ChatGPT. Record client/account/workspace mode, endpoint revision,
   observed negotiated protocol, successful OAuth, discovery and parameterized
   query. Verify null/empty/truncated results, denied writes, reauthorization,
   cancellation, restart and reconnect. Redact tokens and private data.
4. Review permitted data disclosure, role grants and database egress with the
   deployment owner. Obtain the separate deployment/credential approval needed
   for the specific target, then run a limited pilot.

MCP 2026-07-28 requests are stateless. The supported legacy initialize/session
path binds each issued live session to its authenticated principal. Stale legacy
IDs cannot override current-protocol routing. Resume/replay is disabled; no
persistent session or query state survives restart.

Rollback: stop the HTTP process or remove its proxy route and start the same
binary without `--transport http` to retain stdio. Revoke the approved OAuth
client/role separately when needed. Record the exact image/commit and deployment
owner; do not infer deployed compatibility from a successful source merge.
