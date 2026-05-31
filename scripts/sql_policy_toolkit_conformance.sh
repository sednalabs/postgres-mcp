#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TOOLKIT_ROOT="${TOOLKIT_ROOT:-${ROOT_DIR}/../mcp-toolkit-rs}"
KERNEL_ROOT="${KERNEL_ROOT:-${ROOT_DIR}/../mcp-policy-kernel}"
REPORT_PATH="${REPORT_PATH:-${ROOT_DIR}/.tmp/sql_policy_conformance/sql_policy_core_vs_kernel_report.json}"

if [[ ! -x "${TOOLKIT_ROOT}/scripts/sql_policy_kernel_conformance.sh" ]]; then
  echo "missing Toolkit conformance script: ${TOOLKIT_ROOT}/scripts/sql_policy_kernel_conformance.sh" >&2
  echo "set TOOLKIT_ROOT to a local mcp-toolkit-rs checkout" >&2
  exit 2
fi

if [[ ! -d "${KERNEL_ROOT}" ]]; then
  echo "missing policy kernel checkout: ${KERNEL_ROOT}" >&2
  echo "set KERNEL_ROOT to a local mcp-policy-kernel checkout" >&2
  exit 2
fi

(
  cd "$TOOLKIT_ROOT"
  KERNEL_ROOT="$KERNEL_ROOT" ./scripts/sql_policy_kernel_conformance.sh --report "$REPORT_PATH"
)
