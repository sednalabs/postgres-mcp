#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ARTIFACT_DIR_DEFAULT="${ROOT_DIR}/.artifacts/postgres-mcp"
STATE_FILE_DEFAULT="${ARTIFACT_DIR_DEFAULT}/current.json"
ARTIFACT_DIR="${POSTGRES_MCP_ARTIFACT_DIR:-${ARTIFACT_DIR_DEFAULT}}"
STATE_FILE="${POSTGRES_MCP_ARTIFACT_STATE_FILE:-${STATE_FILE_DEFAULT}}"

usage() {
  cat <<'EOF'
Usage:
  scripts/publish_postgres_mcp_artifact.sh <binary_path> [profile]

Description:
  Publish a pre-built postgres-mcp binary as the current runtime artifact.
  This command does NOT build. It only writes the promoted artifact pointer.

Examples:
  scripts/publish_postgres_mcp_artifact.sh ./target/debug/postgres-mcp
  scripts/publish_postgres_mcp_artifact.sh ./target/release/postgres-mcp release
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ $# -lt 1 || $# -gt 2 ]]; then
  usage
  exit 2
fi

BIN_INPUT="$1"
PROFILE_INPUT="${2:-auto}"

if [[ "${BIN_INPUT}" = /* ]]; then
  BIN_PATH="${BIN_INPUT}"
else
  BIN_PATH="${ROOT_DIR}/${BIN_INPUT}"
fi

if ! BIN_PATH="$(realpath "${BIN_PATH}" 2>/dev/null)"; then
  echo "publish_postgres_mcp_artifact: unable to resolve binary path: ${BIN_INPUT}" >&2
  exit 1
fi

if [[ ! -x "${BIN_PATH}" ]]; then
  echo "publish_postgres_mcp_artifact: binary not executable: ${BIN_PATH}" >&2
  exit 1
fi

PROFILE="${PROFILE_INPUT}"
if [[ "${PROFILE}" == "auto" ]]; then
  case "${BIN_PATH}" in
    */target/release/*) PROFILE="release" ;;
    */target/debug/*) PROFILE="debug" ;;
    *) PROFILE="custom" ;;
  esac
fi

json_escape() {
  local raw="$1"
  raw="${raw//\\/\\\\}"
  raw="${raw//\"/\\\"}"
  raw="${raw//$'\n'/\\n}"
  printf '%s' "${raw}"
}

mkdir -p "${ARTIFACT_DIR}"
TMP_FILE="$(mktemp "${ARTIFACT_DIR}/.current.json.tmp.XXXXXX")"
trap 'rm -f "${TMP_FILE}"' EXIT

GIT_SHA="$(git -C "${ROOT_DIR}" rev-parse --short=12 HEAD 2>/dev/null || echo "unknown")"
BUILT_AT_UTC="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
BIN_VERSION="$("${BIN_PATH}" --version 2>/dev/null | head -n 1 || true)"
BUILD_HELPER_TASK_ID="${BUILD_HELPER_TASK_ID:-}"
BUILD_HELPER_PRESET_ID="${BUILD_HELPER_PRESET_ID:-}"
PUBLISHED_BY="${USER:-unknown}"

cat > "${TMP_FILE}" <<EOF
{
  "schema": "postgres_mcp_promoted_artifact",
  "version": 1,
  "binary_path": "$(json_escape "${BIN_PATH}")",
  "profile": "$(json_escape "${PROFILE}")",
  "git_sha": "$(json_escape "${GIT_SHA}")",
  "binary_version": "$(json_escape "${BIN_VERSION}")",
  "built_at_utc": "$(json_escape "${BUILT_AT_UTC}")",
  "build_helper_task_id": "$(json_escape "${BUILD_HELPER_TASK_ID}")",
  "build_helper_preset_id": "$(json_escape "${BUILD_HELPER_PRESET_ID}")",
  "published_by": "$(json_escape "${PUBLISHED_BY}")"
}
EOF

mv "${TMP_FILE}" "${STATE_FILE}"
trap - EXIT

echo "published postgres-mcp artifact:"
echo "  state_file=${STATE_FILE}"
echo "  binary_path=${BIN_PATH}"
echo "  profile=${PROFILE}"
echo "  git_sha=${GIT_SHA}"
