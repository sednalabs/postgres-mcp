# SQL Policy Contract

This document defines the public contract expected from the read-only SQL policy
used by `postgres-mcp`.

## Contract Shape

Policy decisions use stable machine-readable fields:

```json
{
  "allow": true,
  "code": null,
  "reason": null,
  "policy_contract_version": "sql-restricted/v1"
}
```

For denials:

```json
{
  "allow": false,
  "code": "NOT_READ_ONLY_PREFIX",
  "reason": "restricted_sql",
  "policy_contract_version": "sql-restricted/v1"
}
```

Rules:

- `code` and `reason` are compatibility commitments.
- `policy_contract_version` changes when decision shape or code semantics break
  compatibility.
- Runtime policy evaluation flows through the shared toolkit policy authority,
  which records `decision_source`, `runtime_mode`, and
  `policy_contract_version` in conformance reports.
- Restricted-mode policy inputs must also fit the policy-kernel boundary limits;
  oversized policy inputs fail closed even when the general SQL payload limit is
  higher.
- Human-readable messages may improve over time but must remain redacted.

## Local Rebaseline Flow

Run from the repository root:

```bash
./scripts/sql_policy_contract_rebaseline.sh
```

The command verifies local policy-authority alignment and writes:

```text
.tmp/policy_contract_rebaseline/sql_policy_contract_rebaseline.json
```

## Toolkit Conformance

When the companion policy toolkit workspace is available, run:

```bash
./scripts/sql_policy_toolkit_conformance.sh
```

The command writes a report that includes toolkit policy-authority provenance:

```text
.tmp/sql_policy_conformance/sql_policy_core_vs_kernel_report.json
```

## Runtime Differential Conformance

Run the PostgreSQL runtime policy comparison:

```bash
./scripts/sql_policy_conformance_diff.sh
```

The command writes:

```text
.tmp/sql_policy_conformance/sql_policy_conformance_report.json
.tmp/sql_policy_conformance/sql_policy_conformance_artifacts.json
```

## Runtime Envelope Checks

Run DB-backed runtime safety checks when a validation database is available:

```bash
./scripts/runtime_safety_conformance.sh --require-db-runtime
```

The command verifies read-only execution semantics, timeout behavior, and
extension guard envelopes, writing:

```text
.tmp/runtime_safety/runtime_safety_probe_report.json
.tmp/runtime_safety/runtime_safety_artifacts.json
```

## Stability Policy

The SQL policy contract evolves additively by default:

1. Adding a new denial code is allowed within the same major contract track.
2. Removing or renaming a code requires a new contract version.
3. Field-shape changes require a new contract version.
4. Deprecated codes should remain listed with explicit migration guidance before
   removal.

## Rebaseline Triggers

Run a new baseline whenever one of these changes:

- local SQL policy-authority code/reason mapping
- companion policy toolkit authority mapping
- runtime policy error envelope semantics
- SQL vector expectations used by conformance scripts

## Release Evidence

Release bundles should include:

- contract rebaseline report
- toolkit conformance report when applicable
- runtime differential report
- runtime safety report
- exact commit, command, and environment summary for each generated artifact

## Troubleshooting

Contract rebaseline failure:

- inspect the generated report
- confirm companion policy artifacts are present if the script requires them
- rerun after intentional contract updates

Differential conformance mismatch:

- inspect `.tmp/sql_policy_conformance/sql_policy_conformance_report.json`
- treat mismatches as regressions unless an intentional contract change is
  documented
- update vectors and contract docs before rebaseline when behavior changes are
  intentional

Runtime safety failure:

- inspect `.tmp/runtime_safety/runtime_safety_probe_report.json`
- fix read-only, timeout, or guard-envelope behavior before release

## Assurance Boundary

This workflow helps prove:

- policy decision shape stays stable
- runtime and toolkit policy authorities agree with expected vectors
- read-only and timeout envelopes are exercised
- release artifacts preserve enough metadata for review

This workflow does not prove:

- full SQL parser correctness for every dialect edge case
- production DB/network/TLS correctness outside exercised checks
- deployment-specific authorization or data-isolation policy
