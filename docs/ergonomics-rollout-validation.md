# Ergonomics Rollout Validation (Post-Merge)

This document defines the lightweight validation pass for WI `1741` after
ergonomics changes are shipped. The goal is to confirm reduced operator
iteration cost by sampling both harness-based and operator-facing verification
loops.

## Validation checklist

Run in order from the repo root:

1. `./scripts/index_advisor_repro_check.sh`
2. `python3 ./scripts/ergonomics_rollout_validation.py`
3. Capture and commit:
   - `.tmp/index_advisor_v1/index_advisor_repro_report.json`
   - `.tmp/ergonomics_validation/rollout_validation_report.json`
4. Review:
   - repro pack workload pass/fail counts
   - per-scenario error-loop count
   - per-scenario correction latency
5. Publish a recommendation summary:
   - if all scenarios pass and loops stay within budget, proceed to merge/rollout status,
   - if loops or latency regress, create a follow-up issue and attach this report.

## Scenario set

The rollout validation script reads:
- `fixtures/ergonomics_validation/analyst_scenarios.json`

It evaluates provider-neutral operator scenarios inspired by the downstream
verification workflow:
- `Provider triage`
- `Direct-route date counts`
- `Queue-state verification`
- `Landed-date diffing`
- `Bound params single-statement correction`

Each scenario may use either:
- one successful attempt for zero-loop verification checks, or
- one failing attempt plus one corrected attempt for contract-correction lanes.

Fixture attempts can express a full `execute_sql` input object and optional
assertions on returned payload JSON pointers or failure-message substrings. Use
that to keep workflow expectations executable rather than descriptive-only.

## Output interpretation

The JSON report includes scenario summaries like:

- `error_loop_count` = number of failed attempts before success
- `correction_latency_ms` = elapsed time from first attempt start to first successful correction
- `attempts[].error_code` from `execute_sql` failures when present

### Recommended pass thresholds

- Repro pack workload failure count: `0`
- Operator verification loops (`Provider triage`, `Direct-route date counts`,
  `Queue-state verification`, `Landed-date diffing`):
  `error_loop_count = 0`
- Contract-correction lane (`Bound params single-statement correction`):
  `error_loop_count <= 1` and final success must preserve the corrective hint
- Scenario `correction_latency_ms`: context-dependent; treat sustained growth
  as follow-up signal

## Recommendation summary (template)

Use this format for release notes and rollout communication:

| Item | Result | Recommendation |
| --- | --- | --- |
| Repro pack | `pass / fail` | `pass: continue` / `fail: open follow-up issue for parity gap` |
| Provider triage | `error_loop_count / correction_latency_ms` | `continue if loop = 0; investigate readable-row defaults if >0` |
| Direct-route date counts | `error_loop_count / correction_latency_ms` | `continue if loop = 0; investigate default capped fetch path if >0` |
| Queue-state verification | `error_loop_count / correction_latency_ms` | `continue if loop = 0; investigate fast_agent defaults if >0` |
| Landed-date diffing | `error_loop_count / correction_latency_ms` | `continue if loop = 0; investigate readable-row ergonomics if >0` |
| Bound params single-statement correction | `error_loop_count / correction_latency_ms` | `continue if loop <= 1; investigate contract guidance if >1` |
| Overall rollout signal | `go / hold` | `go: proceed with merge posture` / `hold: open follow-up + re-run validation` |

## Build-helper execution

Use the dedicated preset when running the full pass through Build Helper:

```bash
systemd-run ... (via preset)
```

Equivalent preset ID:

```text
postgres-mcp.ergonomics-rollout-validation
```

This runs the same validation command documented above and writes artifacts under
`.tmp/ergonomics_validation`.
