# Parity Tone Style Note

Use neutral, implementation-focused language in parity docs and fixtures.

Preferred patterns:

- say `known differences` rather than evaluative terms
- describe `shape` and `classification` differences without value framing
- use `companion implementation` / `cross-implementation` wording where possible
- keep remediation text concrete and operational (`normalize`, `map`, `group`)

Avoid:

- combative framing (`wrong`, `broken`, `inferior`)
- defensive framing (`must justify`, `cannot trust`) when a neutral statement is sufficient
- wording that implies one implementation is authoritative for all presentation choices

Schema rule:

- Do not rename fixture schema fields (`known_differences`, `comparison_mode`,
  `known_difference_ids`) as part of wording-only edits.
