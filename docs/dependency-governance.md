# Dependency Governance

This document defines dependency selection and upgrade policy for `postgres-mcp`.

## Goal

Keep the server secure, maintainable, and cold-start efficient by preferring highly regarded, actively maintained crates with clear operational risk signals.

## Scope

- Direct dependencies in `Cargo.toml`
- Tooling dependencies used in release checks
- Major and minor dependency upgrades

## Go/No-Go Criteria

All new direct crates and major upgrades must meet every hard gate below.

1. `security`: No unresolved RustSec advisory for selected version.
2. `license`: License is accepted by `deny.toml`.
3. `source`: Registry source is trusted (`crates.io` only by default).
4. `maintenance`: Evidence of active maintenance (recent releases, active issue/PR activity, non-abandoned project).
5. `adoption/reputation`: Evidence the crate is broadly used or maintained by a trusted team/project.
6. `cold-start impact`: No avoidable startup-path regressions; dependency should not force eager heavy init on default server startup.
7. `fit`: Clear justification that existing dependencies or stdlib cannot solve the need with lower risk.

If any hard gate fails, the change is `no-go` unless an explicit, time-bounded exception is approved and documented.

## Required Evidence for Dependency Changes

Every dependency change (new crate, removed crate, major/minor upgrade) must include a policy note in the associated PR or issue.

Use this template:

```text
Dependency change note
- crate: <name> <old -> new>
- change type: <new | upgrade | removal>
- purpose: <why needed>
- alternatives considered: <stdlib/existing crates/other crates>
- maintenance evidence: <release recency + repo activity>
- adoption/reputation evidence: <reverse-deps/downloads/known users or maintainer org>
- security status: <cargo deny + cargo audit result>
- license status: <accepted license(s)>
- startup impact: <expected effect on cold start/steady state>
- rollback plan: <how to revert safely>
- exception (if any): <risk accepted, approver, expiry date>
```

## Enforcement

Run:

```bash
./scripts/dependency_governance_check.sh
```

The script enforces:

1. advisory/license/source policy via `cargo-deny`
2. RustSec check via `cargo-audit`
3. stale-risk scan on direct dependencies via `cargo-outdated`

Default mode treats outdated direct dependencies as a failing gate. For local exploration only, you can run with:

```bash
STRICT_OUTDATED=0 ./scripts/dependency_governance_check.sh
```

## Exceptions

Exceptions are allowed only when there is a clear delivery blocker and no safer near-term option.

Exception requirements:

1. Documented in a PR or issue with maintainer and explicit expiry date.
2. Bounded duration (target <= 30 days).
3. Follow-up item created before merge for exception removal.

## Remote HTTP dependency review (2026-11-06)

The HTTP migration uses toolkit commit
[`3ade735cf993892dd5f6c6808b0e5ad473e83dc2`](https://github.com/sednalabs/mcp-toolkit-rs/commit/3ade735cf993892dd5f6c6808b0e5ad473e83dc2),
the narrow PostgreSQL root-certificate correction in
[toolkit PR #346](https://github.com/sednalabs/mcp-toolkit-rs/pull/346) based on
previously qualified `58bb4d13e09752a30911ad62bf2c35b6a51bb7d1`,
and the unchanged reviewed RMCP 3.9.0 runtime/macros source
[`31e83f4f752001d2afc386254a4d17b5eab5f419`](https://github.com/sednalabs/rust-sdk/commit/31e83f4f752001d2afc386254a4d17b5eab5f419).
Both root patches are necessary because dependency workspace patches do not
propagate. `deny.toml` allows only those two Git repositories and requires `rev`
specifications. Reviewers must verify the exact manifest and lockfile revisions
together; the source allowance is not permission to follow an unreviewed branch.
The SDK supplies the verified Origin/protocol transport hardening rather than
duplicating it in this server. Toolkit supplies OAuth verification, HTTP server
composition, and bounded native-task authority. OAuth discovery and HTTP server
startup are not run on the ordinary stdio path.

The first hosted dependency scan of this migration identified two advisories and
a yanked crate. The corrected resolution upgrades rather than suppresses them:

- `anyhow` 1.0.102 → 1.0.104 fixes
  [RUSTSEC-2026-0190](https://rustsec.org/advisories/RUSTSEC-2026-0190);
  the manifest minimum is 1.0.103.
- `tokio` 1.52.3 → 1.54.1 fixes
  [RUSTSEC-2026-0386](https://rustsec.org/advisories/RUSTSEC-2026-0386);
  the manifest minimum is 1.53.3.
- Transitive `chacha20` 0.10.0 → 0.10.2 removes the yanked release.
- Compatible direct dependency updates were resolved in the same lockfile to
  satisfy the existing stale-risk gate. The security refresh itself did not move toolkit/SDK revisions. The separate
  toolkit correction below changes only its PostgreSQL DSN adapter.

The only new license allowance is restricted to `webpki-root-certs =1.0.11` and
`CDLA-Permissive-2.0`, matching the certificate-root policy in the pinned toolkit.
This target-specific Mozilla trust-anchor data is used by
`rustls-platform-verifier` through the OAuth HTTP client. The registry archive
SHA-256 was checked against the lockfile
(`e49d0e65abc00c142e8f9428da991da9f1a250c39be97459901eb12b9b49f62a`),
and its packaged manifest and license were inspected. The complete license is
retained in `THIRD_PARTY_NOTICES.md`. This is not a blanket license allowance or
an advisory exception. Review changes to the package, version, provenance, or
redistribution obligations before extending it.

No advisory ignores or yanked-package exceptions were added. Preserve all three
existing governance gates and rerun them against the final hosted PR head before
merge. Rollback must retain patched dependency versions; reverting to the
vulnerable or yanked resolution is not an acceptable rollback.

The PR #346 correction removes validated `sslrootcert` only from the downstream
`tokio-postgres` DSN (URI and keyword forms, every TLS mode); the connector keeps
the original root path and the normalized source DSN. Previously the driver
rejected this toolkit-only option before connecting. Focused upstream tests cover
driver parsing, mode preservation, default/empty/mixed-case inputs, and duplicate
root rejection. This does not weaken verified TLS or introduce a protocol fork.
Run the full hosted toolkit gates and consumer custom-CA database contracts on
the exact corrective source before landing.
