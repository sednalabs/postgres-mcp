# Contributing

Thanks for working on `postgres-mcp`. This repository is public-facing, so
keep examples, docs, commit messages, and CI text neutral and portable.

## Development Principles

- Preserve tool names and payload contracts unless an intentional migration is
  planned.
- Prefer small, reversible diffs.
- Keep runtime behavior in the module that already owns the concern.
- Do not commit generated artifacts, local configuration, secrets, raw database
  URIs, or machine-specific environment details.
- Use placeholder credentials and localhost examples in public docs.

## Before Editing

1. Read `AGENTS.md` for repository-local instructions.
2. Check the current branch and worktree state:

```bash
git status --short --branch
```

3. If the branch tracks a remote, refresh refs before substantial work:

```bash
git fetch origin --prune
```

## Validation

Use the smallest validation that proves the change.

For Rust behavior changes, prefer the documented Build Helper MCP presets when
available:

```text
postgres-mcp.test
postgres-mcp.build
postgres-mcp.deferred-tool-discovery-smoke
postgres-mcp.integration-matrix-check
```

Local fallback commands:

```bash
cargo test
cargo build
./scripts/deferred_tool_discovery_smoke.sh --startup-db-connect=background
```

For docs-only changes:

```bash
git diff --check
```

Run additional link or wording scans when changing public documentation.

## Contract-Sensitive Changes

When changing tool args, output schemas, or response envelopes:

1. Update implementation and tests.
2. Rebaseline `spec/tool_schema_snapshot.v1.json` only when the schema change is
   intentional.
3. Update `README.md`, [Tool Guide](docs/TOOL_GUIDE.md), and any compatibility
   docs that describe the affected behavior.
4. Call out compatibility impact in the PR description.

## Public Wording

Before opening or updating a public PR, scan the staged diff, PR title/body, and
recent commit messages for:

- hostnames, usernames, email addresses, credentials, tokens, or machine-local paths
- non-public repository names or non-public release names
- deployment-specific infrastructure details
- wording that exposes non-public project context

Prefer generic terms such as "companion workspace", "maintainer-configured
checks", "publication policy", and "public examples".
