#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROMOTED_STATE_FILE_DEFAULT="${ROOT_DIR}/.artifacts/postgres-mcp/current.json"
PROMOTED_STATE_FILE="${POSTGRES_MCP_ARTIFACT_STATE_FILE:-${PROMOTED_STATE_FILE_DEFAULT}}"

extract_json_string() {
  local key="$1"
  local file="$2"
  sed -n "s/^[[:space:]]*\"${key}\"[[:space:]]*:[[:space:]]*\"\\(.*\\)\"[[:space:]]*,\{0,1\}[[:space:]]*$/\\1/p" "${file}" | head -n 1
}

BIN_CANDIDATE="${POSTGRES_MCP_BIN:-}"
if [[ -n "$BIN_CANDIDATE" && -x "$BIN_CANDIDATE" ]]; then
  BIN="$BIN_CANDIDATE"
elif [[ -f "${PROMOTED_STATE_FILE}" ]]; then
  PROMOTED_BIN="$(extract_json_string "binary_path" "${PROMOTED_STATE_FILE}")"
  if [[ -n "${PROMOTED_BIN}" && -x "${PROMOTED_BIN}" ]]; then
    BIN="${PROMOTED_BIN}"
  fi
fi

if [[ -z "${BIN:-}" ]]; then
  BIN_CANDIDATE="./target/release/postgres-mcp"
  if [[ -x "$BIN_CANDIDATE" ]]; then
    BIN="$BIN_CANDIDATE"
  elif [[ -x "./target/debug/postgres-mcp" ]]; then
    BIN="./target/debug/postgres-mcp"
  else
    echo "owner_diagnostic_snapshot: postgres-mcp binary not found."
    echo "Build first (for example: cargo build --release)."
    exit 1
  fi
fi

TIMESTAMP_UTC="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
TOOL_TMP="$(mktemp)"
trap 'rm -f "$TOOL_TMP"' EXIT

"$BIN" --print-tools >"$TOOL_TMP"
TOOLS_HASH="$(sha256sum "$TOOL_TMP" | awk '{print $1}')"
TOOLS_JSON="$(cat "$TOOL_TMP")"

safe_var() {
  local key="$1"
  local value="${!key-}"
  if [[ -z "${value}" ]]; then
    echo "<unset>"
  else
    echo "${value}"
  fi
}

echo "timestamp_utc=${TIMESTAMP_UTC}"
echo "binary_path=${BIN}"
if [[ -f "${PROMOTED_STATE_FILE}" ]]; then
  echo "promoted_artifact_state_file=${PROMOTED_STATE_FILE}"
  echo "promoted_artifact_binary=$(extract_json_string "binary_path" "${PROMOTED_STATE_FILE}")"
else
  echo "promoted_artifact_state_file=<missing>"
fi
echo "binary_version=$("$BIN" --version | head -n 1)"
echo "tool_schema_sha256=${TOOLS_HASH}"
echo "database_uri_set=$([[ -n "${DATABASE_URI-}" ]] && echo "true" || echo "false")"
echo "startup_role=$(safe_var POSTGRES_MCP_STARTUP_ROLE)"
echo "startup_db_connect=$(safe_var POSTGRES_MCP_STARTUP_DB_CONNECT)"
echo "startup_coordination_mode=$(safe_var POSTGRES_MCP_STARTUP_COORDINATION_MODE)"
echo "startup_dependency_mode=$(safe_var POSTGRES_MCP_STARTUP_DEPENDENCY_MODE)"
echo "startup_required_relations=$(safe_var POSTGRES_MCP_STARTUP_REQUIRED_RELATIONS)"
echo "metadata_policy_mode=$(safe_var POSTGRES_MCP_METADATA_POLICY_MODE)"
echo "circuit_breaker_enabled=$(safe_var POSTGRES_MCP_CIRCUIT_BREAKER_ENABLED)"
echo "circuit_breaker_threshold=$(safe_var POSTGRES_MCP_CIRCUIT_BREAKER_FAILURE_THRESHOLD)"
echo "circuit_breaker_cooldown_sec=$(safe_var POSTGRES_MCP_CIRCUIT_BREAKER_COOLDOWN_SEC)"
echo "backpressure_base_ms=$(safe_var POSTGRES_MCP_BACKPRESSURE_BASE_MS)"
echo "backpressure_cap_ms=$(safe_var POSTGRES_MCP_BACKPRESSURE_CAP_MS)"
echo "tool_names_json=${TOOLS_JSON}"
