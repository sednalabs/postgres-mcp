#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_PATH="${1:-${ROOT_DIR}/target/debug/postgres-mcp}"

if [[ ! -x "${BIN_PATH}" ]]; then
  echo "building debug binary for parity check..." >&2
  (cd "${ROOT_DIR}" && cargo build --quiet)
fi

tools_json="$(${BIN_PATH} --print-tools)"

expected_tools=(
  "list_schemas"
  "list_objects"
  "get_object_details"
  "explain_query"
  "execute_sql"
  "analyze_workload_indexes"
  "analyze_query_indexes"
  "analyze_db_health"
  "get_top_queries"
)

missing=0
for tool in "${expected_tools[@]}"; do
  if ! grep -q "\"${tool}\"" <<<"${tools_json}"; then
    echo "missing tool: ${tool}" >&2
    missing=1
  fi
done

for tool in $(grep -o '"[^"]*"' <<<"${tools_json}" | tr -d '"'); do
  found=0
  for expected in "${expected_tools[@]}"; do
    if [[ "${tool}" == "${expected}" ]]; then
      found=1
      break
    fi
  done
  if [[ ${found} -eq 0 ]]; then
    echo "unexpected tool: ${tool}" >&2
    missing=1
  fi
done

if [[ ${missing} -ne 0 ]]; then
  echo "parity check failed" >&2
  exit 1
fi

echo "tool parity check passed (${#expected_tools[@]} tools)"
