#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ARTIFACT_DIR_DEFAULT="${ROOT_DIR}/.artifacts/postgres-mcp"
STATE_FILE_DEFAULT="${ARTIFACT_DIR_DEFAULT}/current.json"
STATE_FILE="${POSTGRES_MCP_ARTIFACT_STATE_FILE:-${STATE_FILE_DEFAULT}}"

extract_json_string() {
  local key="$1"
  local file="$2"
  sed -n "s/^[[:space:]]*\"${key}\"[[:space:]]*:[[:space:]]*\"\\(.*\\)\"[[:space:]]*,\{0,1\}[[:space:]]*$/\\1/p" "${file}" | head -n 1
}

fail_missing_artifact() {
  echo "launch_postgres_mcp_from_promoted: no built postgres-mcp binary found." >&2
  echo "expected one of:" >&2
  echo "  ${ROOT_DIR}/target/debug/postgres-mcp" >&2
  echo "  ${ROOT_DIR}/target/release/postgres-mcp" >&2
  echo "no build is performed by this launcher." >&2
  echo "build first via Build Helper MCP, then restart." >&2
  exit 1
}

declare -A seen_paths=()
declare -a candidates=()

add_candidate() {
  local candidate="$1"
  if [[ -z "${candidate}" || ! -x "${candidate}" ]]; then
    return
  fi
  if [[ -z "${seen_paths["${candidate}"]+x}" ]]; then
    seen_paths["${candidate}"]=1
    candidates+=("${candidate}")
  fi
}

if [[ -f "${STATE_FILE}" ]]; then
  promoted_path="$(extract_json_string "binary_path" "${STATE_FILE}")"
  add_candidate "${promoted_path}"
fi

add_candidate "${ROOT_DIR}/target/debug/postgres-mcp"
add_candidate "${ROOT_DIR}/target/release/postgres-mcp"

if [[ ${#candidates[@]} -eq 0 ]]; then
  fail_missing_artifact
fi

BINARY_PATH=""
NEWEST_MTIME=0
for candidate in "${candidates[@]}"; do
  mtime="$(stat -c '%Y' "${candidate}" 2>/dev/null || echo 0)"
  if [[ "${mtime}" =~ ^[0-9]+$ ]] && (( mtime >= NEWEST_MTIME )); then
    NEWEST_MTIME="${mtime}"
    BINARY_PATH="${candidate}"
  fi
done

if [[ -z "${BINARY_PATH}" ]]; then
  fail_missing_artifact
fi

exec "${BINARY_PATH}" "$@"
