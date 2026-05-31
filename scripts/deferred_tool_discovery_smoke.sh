#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LAUNCHER="${POSTGRES_MCP_LAUNCHER:-${ROOT_DIR}/scripts/launch_postgres_mcp_from_promoted.sh}"

require_execute_sql=0
declare -a launcher_args=()

usage() {
  cat >&2 <<'EOF'
Usage: scripts/deferred_tool_discovery_smoke.sh [--expose-execute-sql] [-- <launcher args>]

Validates that the configured Postgres MCP launcher can print a non-empty,
deferred-discovery-safe tool inventory. The launcher must already have a built
binary available; this smoke intentionally does not build.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --expose-execute-sql)
      require_execute_sql=1
      launcher_args+=("--expose-execute-sql")
      shift
      ;;
    --)
      shift
      launcher_args+=("$@")
      break
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      launcher_args+=("$1")
      shift
      ;;
  esac
done

if [[ ! -x "${LAUNCHER}" ]]; then
  echo "deferred_tool_discovery_smoke: launcher is not executable: ${LAUNCHER}" >&2
  exit 1
fi

stdout_file="$(mktemp)"
stderr_file="$(mktemp)"
trap 'rm -f "${stdout_file}" "${stderr_file}"' EXIT

if ! "${LAUNCHER}" "${launcher_args[@]}" --print-tools >"${stdout_file}" 2>"${stderr_file}"; then
  echo "deferred_tool_discovery_smoke: launcher failed before tool discovery." >&2
  sed -n '1,80p' "${stderr_file}" >&2
  exit 1
fi

python3 - "${stdout_file}" "${require_execute_sql}" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
require_execute_sql = sys.argv[2] == "1"

try:
    payload = json.loads(path.read_text(encoding="utf-8"))
except json.JSONDecodeError as exc:
    raise SystemExit(f"tool inventory is not valid JSON: {exc}") from exc

if not isinstance(payload, list):
    raise SystemExit("tool inventory must be a JSON array")

tools = [item for item in payload if isinstance(item, str)]
if len(tools) != len(payload):
    raise SystemExit("tool inventory must contain only tool-name strings")
if not tools:
    raise SystemExit("tool inventory is empty")

required_tools = {
    "list_schemas",
    "list_objects",
    "get_object_details",
    "query_sql",
    "query_tuples",
    "render_sql",
    "export_sql",
    "explain_query",
    "analyze_db_health",
    "analyze_query_indexes",
    "analyze_workload_indexes",
}
if require_execute_sql:
    required_tools.add("execute_sql")

missing = sorted(required_tools - set(tools))
if missing:
    raise SystemExit("tool inventory missing required tools: " + ", ".join(missing))

if "execute_sql" in tools and not require_execute_sql:
    raise SystemExit(
        "execute_sql is discoverable by default; keep it hidden unless explicitly exposed"
    )

print(
    "deferred tool discovery smoke passed "
    f"({len(tools)} tools; execute_sql_required={str(require_execute_sql).lower()})"
)
PY
