#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TOP_FAILURES="${INTEGRATION_MATRIX_PRINT_TOP_FAILURES:-5}"
COMPOSE_BUILD_POLICY="${INTEGRATION_MATRIX_COMPOSE_BUILD_POLICY:-auto}"

exec "${ROOT_DIR}/scripts/integration_matrix_check.sh" \
  --with-compose \
  --compose-build-policy "${COMPOSE_BUILD_POLICY}" \
  --print-top-failures "${TOP_FAILURES}" \
  "$@"
