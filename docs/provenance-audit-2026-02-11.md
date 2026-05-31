# Provenance Audit (2026-02-11)

## Scope

Audit objective: verify that `postgres-mcp` remains an independent Rust
implementation while using `crystaldba/postgres-mcp` as a behavioral reference.

Compared repositories:

- Inspiration repo: <https://github.com/crystaldba/postgres-mcp>
- Local repo: `servers/postgres-mcp`

Commit snapshots used for this audit:

- `crystaldba/postgres-mcp`: `07eb329c8c48e49640e0d1b5b35465d4d024c3ee`
- `servers/postgres-mcp`: `36c42c8ffa575e0cf2d478d2f3a5daf18d177de9`

## Methods

1. Exact file-content hash comparison for textual sources (excluding build
   artifacts and binary assets).
2. README long-line overlap scan.
3. Near-similarity file-pair scan (`difflib` ratio).
4. Focused review of any overlapping long lines.

## Results

1. Exact textual file matches: `0`.
2. README long-line overlaps (>= 40 chars): `0`.
3. Near-similarity highest ratio observed: `0.082` (very low).
4. Shared lines identified were limited to expected SQL/tool-contract parity
   expressions, for example:
   - extension status probes against `pg_extension` and
     `pg_available_extensions`
   - schema classification labels (`System Schema`,
     `System Information Schema`, `User Schema`)

## Assessment

Current evidence supports independent implementation with low copying risk.
Behavior-level parity intent is explicit and appropriate for compatibility.

## Follow-up Controls

1. Keep legacy parity fixture provenance notes up to date when refreshing `fixtures/parity_v2/`.
2. Keep this audit note when doing major parity fixture refreshes.
3. Preserve `THIRD_PARTY_NOTICES.md` attribution for referenced upstream
   project metadata and license context.
