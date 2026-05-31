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
