# AGENTS.md - postgres-mcp

## Scope and precedence
- These instructions apply to this repository.
- If a closer `AGENTS.md` is added later, the closer file wins for that scope.
- This file adds repo-local guidance and does not weaken workspace architecture policy.

## Operating intent
- Maintain a secure, low-latency Rust MCP server for PostgreSQL.
- Preserve practical tool compatibility while improving ergonomics and operability.
- Keep the default path simple: predictable contracts, explicit errors, and minimal surprises.

## Public-facing collaboration defaults
- Assume all docs and examples may be read publicly.
- Use placeholder credentials and localhost examples in documentation.
- Never include secrets, tokens, or raw connection URIs in committed files.
- Keep language neutral and portable (avoid host-specific internal runbook assumptions in core docs).

## Repo hygiene
- Do not commit generated artifacts:
  - `target/`
  - `.tmp/`
- Keep machine-local runtime configuration in environment variables or service wrappers, not committed files.

## Implementation boundaries
- `src/tools/*`: MCP tool behavior and payload shaping.
- `src/db.rs`: database transport and query execution primitives.
- `src/config.rs`: CLI/env parsing and startup configuration behavior.
- `src/server.rs`: server wiring and router exposure.
- Prefer adding logic to the bounded module that already owns that concern.

## Design requirements
- Favor small, reversible diffs over broad rewrites.
- Preserve tool names and contract stability unless an intentional migration is planned.
- Keep error messages actionable but concise.
- Prefer explicit contracts over implicit behavior; avoid hidden coupling.

## The Principle of an Elegant Solution
Prefer solutions that are:
1. Small and direct.
2. Incremental and reversible.
3. Easy to understand for new contributors.
4. Backed by clear tooling and tests.
5. Consistent with existing architecture and repository conventions.

## Contract-sensitive changes
When changing tool args, output schemas, or envelope behavior:
1. Update code and tests.
2. Rebaseline `spec/tool_schema_snapshot.v1.json` only when the change is intentional.
3. Update docs that define behavior (`README.md`, and `docs/payload-v2-contract.md` when legacy v2 fixture behavior is in scope).
4. Call out compatibility impact in PR/work-item notes.

## Testing and verification
- For behavior changes, tests are required before handoff.
- On shared hosts, prefer Build Helper MCP presets (see `docs/build_helper_mcp.md`):
  - `postgres-mcp.test`
  - `postgres-mcp.build`
  - `postgres-mcp.build-release`
- Use `postgres-mcp.test-update-tool-snapshots` only for intentional schema snapshot updates.
- If Build Helper MCP is unavailable, direct `cargo` commands are acceptable fallback for local validation.

## Documentation upkeep
- Keep `README.md` aligned with current tool arguments and operational defaults.
- Keep contract docs aligned with observed payload behavior:
  - current public surface: `README.md`
  - legacy v2 fixture contract: `docs/payload-v2-contract.md`
- Keep release workflow docs current:
  - `docs/release-checklist.md`
  - `docs/build_helper_mcp.md`
- Follow the Lean documentation style used in the workspace toolkits:
  - module docs explain rationale and security boundaries;
  - public API docs focus on behavior, errors, and security expectations.

## Security and privacy
- Treat SQL and DB connectivity as high-risk surfaces.
- Do not leak secrets in logs, errors, fixtures, or screenshots.
- Preserve fail-closed transport and TLS behavior unless explicitly changing policy.
- For public issue reports, redact hostnames, usernames, and private infrastructure identifiers.
