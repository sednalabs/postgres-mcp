# Safety Checklist

Use this checklist before canary, release promotion, or production cutover.

## Parity and Behavior Safety

- [ ] Contract parity tools are in sync.
- [ ] Semantic parity harness reports no unexpected failures.
- [ ] Known differences are documented and reviewed for compatibility impact.
- [ ] Integration matrix high-severity scenarios pass for the target
  environment.

## SQL and Extension Safety

- [ ] Restricted SQL policy remains fail-closed for write/admin statements.
- [ ] Runtime safety probe passes with DB runtime enabled:
  `./scripts/runtime_safety_conformance.sh --require-db-runtime`.
- [ ] Runtime safety report confirms read-only envelope and timeout enforcement.
- [ ] Extension-dependent tools return stable `code/reason` contracts when
  unavailable.
- [ ] Extension probe failures are triaged before cutover.
- [ ] DB permissions for extension probes are validated in the deployment
  environment.

## Performance Safety

- [ ] Performance gate report exists at `.tmp/perf/perf_gate_report.json`.
- [ ] Startup gate passes.
- [ ] DB-backed gates pass or are explicitly non-gating by release policy.
- [ ] Threshold overrides are documented in rollout notes.

## Observability and Incident Safety

- [ ] Tool responses include actionable `code/reason` for policy and extension
  failures.
- [ ] Incident triage captures tool name, code, reason, and report paths.
- [ ] Redaction expectations are validated.
- [ ] Rollback criteria are documented in [Runbook](RUNBOOK.md).
- [ ] Recent reliability exercise evidence is available when required by the
  release scope.
- [ ] Open remediation issues have assigned maintainers and planned follow-up.
- [ ] Maintainer diagnostic snapshot is attached to release evidence when
  runtime behavior changes.

## Rollback Readiness

- [ ] Fallback deployment artifact or previous release is available.
- [ ] Rollback commands have been rehearsed at least once for high-risk
  releases.
- [ ] Decision authority is assigned for the cutover window.
