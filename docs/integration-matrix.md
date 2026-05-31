# Integration Matrix (v1)

This matrix validates runtime behavior across:

- PostgreSQL major versions (`15`, `18`)
- Extension states (`full`, `missing`, `degraded`)
- Failure modes (auth, network, permission, extension lifecycle)

## Commands

Run with local Docker-backed matrix:

```bash
./scripts/integration_matrix_check.sh --with-compose
```

If Docker requires sudo:

```bash
DOCKER_CMD='sudo docker' ./scripts/integration_matrix_check.sh --with-compose
```

Run against external targets:

```bash
export MATRIX_DB_URI_PG15_FULL='postgresql://user:pass@host:5432/db'
export MATRIX_DB_URI_PG18_FULL='postgresql://user:pass@host:5432/db'
export MATRIX_DB_URI_PG18_FULL_LIMITED='postgresql://user:pass@host:5432/db'
export MATRIX_DB_URI_PG18_MISSING_EXT='postgresql://user:pass@host:5432/db'
export MATRIX_DB_URI_PG18_PGSTAT_DEGRADED='postgresql://user:pass@host:5432/db'
./scripts/integration_matrix_check.sh --fail-on high
```

Output report:

- `.tmp/integration_matrix_v1/integration_matrix_report.json`

Readiness summary report:

```bash
python3 ./scripts/ci_parity_readiness_report.py \
  --report .tmp/integration_matrix_v1/integration_matrix_report.json
```

Readiness outputs:

- `.tmp/integration_matrix_v1/ci_parity_readiness_report.json`
- `.tmp/integration_matrix_v1/ci_parity_readiness_report.md`

## Release gate

By default, the harness fails when any `high` severity scenario fails
(`--fail-on high`).

Current `high` scenarios include:

- full-target schema smoke
- extension missing contract assertions (`EXTENSION_UNAVAILABLE`)
- extension degraded runtime diagnostics
- permission-denied path
- auth/network failure redaction checks
- execute_sql contract metadata, cursor metadata, and summary-only checks

## Expectation contract

Matrix expectations can assert two different layers:

- `envelope_kind`: validates the top-level payload shape (`object`, `list`, `null`).
- `payload_kind`: validates the semantic tool payload shape.

`payload_kind` behavior:

- For v2 success envelopes (`{"ok": true, "data": ...}`), `payload_kind` (and `min_items`)
  is evaluated against `payload.data`.
- For non-v2 payloads (including error payloads), `payload_kind` is evaluated against the
  top-level payload.

Recommended usage:

- Use `envelope_kind: object` when asserting `meta`, `meta_required`, or `data_is_null`.
- Use `payload_kind: list` for row-returning `execute_sql` success scenarios.
- Use `payload_kind: null` for `summary_only` scenarios where `data` is intentionally null.

Expectation linting:

- The harness rejects contradictory combinations such as:
  - `payload_kind: list` with `data_is_null: true`
  - `payload_kind: object` with `data_is_null: true`
  - `payload_kind: null` with `data_is_null: false`

## Assets

- Matrix fixture: `fixtures/integration_matrix_v1/matrix.json`
- Compose matrix: `fixtures/integration_matrix_v1/docker-compose.yml`
