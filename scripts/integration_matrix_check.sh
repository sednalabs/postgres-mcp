#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export PYTHONDONTWRITEBYTECODE="${PYTHONDONTWRITEBYTECODE:-1}"
DEFAULT_WITH_COMPOSE="${INTEGRATION_MATRIX_DEFAULT_WITH_COMPOSE:-1}"
has_with_compose=0

for arg in "$@"; do
  if [[ "$arg" == "--with-compose" ]]; then
    has_with_compose=1
    break
  fi
done

if [[ "${DEFAULT_WITH_COMPOSE}" == "1" && "${has_with_compose}" -eq 0 ]]; then
  exec python3 "${ROOT_DIR}/scripts/integration_matrix_check.py" --with-compose "$@"
fi

exec python3 "${ROOT_DIR}/scripts/integration_matrix_check.py" "$@"
