# Compatibility and Deprecation Lifecycle

This document defines the compatibility governance policy for legacy discovery
inputs (for example `list_objects.name_like`) and other behavior-affecting
contract changes.

## Objectives

- Preserve predictable client behavior while evolving ergonomics.
- Prevent silent breakage caused by long-lived aliases and implicit semantics.
- Require explicit migration artifacts before removal or behavior shifts.

## Scope

- Discovery request arguments (`name_like`, canonical name filters, cursor fields).
- Response envelope fields in v2 (`ok/data/meta`, `ok/error/meta`).
- Error code/reason taxonomy and compatibility expectations.

## Lifecycle Phases

1. `active`
- Legacy input is supported and documented.
- Canonical replacement is available and covered by tests.
- Contract docs include deterministic mapping behavior.

2. `warning`
- Release notes include migration guidance and examples.
- Docs mark legacy input as non-preferred.
- Compatibility tests must continue to pass for both old and new inputs.

3. `removal_candidate`
- At least two stable releases with canonical coverage completed.
- Migration examples and parity notes are published.
- A concrete removal proposal (timeline + impact) is documented before action.

4. `removed`
- Legacy input is removed intentionally.
- Contract docs and known-difference records are updated in the same change.
- Release notes include upgrade steps and fallback guidance.

## Required Evidence Before Phase Progression

- Deterministic contract behavior documented in:
  - `README.md`
  - `docs/payload-v2-contract.md` when legacy v2 fixture compatibility is in scope
- Regression coverage includes old/new path behavior and conflict semantics.
- Release note entry contains:
  - what changed
  - who is affected
  - migration examples
  - rollback guidance when relevant.

## Governance Rules

- No behavior-affecting contract change without explicit compatibility notes.
- No silent semantic flips for existing arguments.
- No removal without prior warning phase evidence.
- Any intentional parity difference must be reflected in
  `fixtures/parity_v2/known_differences.json`.

## Current State (2026-02-28)

- `list_objects.name_like` is in `active` phase with deterministic mapping:
  - no unescaped wildcard => `contains`
  - unescaped wildcard present => `pattern`
- Canonical preferred inputs:
  `name_exact`, `name_prefix`, `name_contains`, `name_pattern`.
