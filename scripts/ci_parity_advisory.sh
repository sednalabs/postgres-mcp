#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

BASE_REF="${1:-origin/main}"
if git rev-parse --verify "${BASE_REF}" >/dev/null 2>&1; then
  DIFF_RANGE="${BASE_REF}...HEAD"
elif git rev-parse --verify HEAD~1 >/dev/null 2>&1; then
  DIFF_RANGE="HEAD~1...HEAD"
else
  DIFF_RANGE=""
fi

if [ -n "${DIFF_RANGE}" ]; then
  CHANGED_FILES="$(git diff --name-only "${DIFF_RANGE}")"
else
  CHANGED_FILES="$(git ls-files)"
fi

if ! printf '%s\n' "${CHANGED_FILES}" | rg -q \
  '^(Cargo.toml|Cargo.lock|src/|scripts/integration_matrix_check\.py|scripts/integration_matrix_check\.sh|scripts/run_canary_parity\.sh|fixtures/integration_matrix_v1/|\.github/workflows/mcp-surface-canary\.yml)'; then
  echo "[advisory] Canary parity not required for this diff."
  exit 0
fi

echo "[advisory] Canary-sensitive files changed."
echo "[advisory] Preferred shared-host lane: Build Helper preset 'postgres-mcp.integration-matrix-check'."

if [ -n "${POSTGRES_MCP_BUILD_HELPER_RUNNER:-}" ] && command -v "${POSTGRES_MCP_BUILD_HELPER_RUNNER}" >/dev/null 2>&1; then
  echo "[advisory] Running Build Helper preset through ${POSTGRES_MCP_BUILD_HELPER_RUNNER}."
  "${POSTGRES_MCP_BUILD_HELPER_RUNNER}" "postgres-mcp.integration-matrix-check"
  exit $?
fi

if [ "${POSTGRES_MCP_ADVISORY_NO_EXEC:-0}" = "1" ]; then
  echo "[advisory] POSTGRES_MCP_ADVISORY_NO_EXEC=1; skipping parity execution."
  echo "[advisory] Local fallback command: ./scripts/run_canary_parity.sh"
  exit 0
fi

echo "[advisory] Build Helper runner not configured; executing local fallback."
exec ./scripts/run_canary_parity.sh
