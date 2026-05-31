#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_PATH="${1:-${ROOT_DIR}/target/release/postgres-mcp}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
PROFILE_HELPER="${ROOT_DIR}/scripts/perf_metrics_profile.py"
REPORT_PATH="${REPORT_PATH:-${ROOT_DIR}/.tmp/perf/perf_gate_report.json}"
RUNS="${RUNS:-25}"

STARTUP_MAX_P50_MS="${STARTUP_MAX_P50_MS:-50}"
STARTUP_MAX_P95_MS="${STARTUP_MAX_P95_MS:-100}"
FIRST_CALL_MAX_P50_MS="${FIRST_CALL_MAX_P50_MS:-250}"
FIRST_CALL_MAX_P95_MS="${FIRST_CALL_MAX_P95_MS:-500}"
STRESSED_PATH_MAX_P50_MS="${STRESSED_PATH_MAX_P50_MS:-900}"
STRESSED_PATH_MAX_P95_MS="${STRESSED_PATH_MAX_P95_MS:-1800}"

REQUIRE_DB_SCENARIOS="${REQUIRE_DB_SCENARIOS:-1}"
STRESSED_PROBE_REPEAT="${STRESSED_PROBE_REPEAT:-3}"
ALLOW_SHELL_OVERRIDE="${ALLOW_SHELL_OVERRIDE:-0}"

FIRST_CALL_SQL="${FIRST_CALL_SQL:-SELECT 1}"
STRESSED_PATH_SQL="${STRESSED_PATH_SQL:-SELECT md5(i::text) FROM generate_series(1,10000) AS i}"

if [[ ! -x "${BIN_PATH}" ]]; then
  echo "building release binary for benchmark..." >&2
  (cd "${ROOT_DIR}" && nice -n 19 cargo build --release --quiet)
fi

if [[ ! -f "${PROFILE_HELPER}" ]]; then
  echo "missing profile helper: ${PROFILE_HELPER}" >&2
  exit 2
fi

mkdir -p "$(dirname "${REPORT_PATH}")"
SCENARIO_NDJSON="$(mktemp)"
trap 'rm -f "${SCENARIO_NDJSON}"' EXIT

measure_ms_argv() {
  local start_ns end_ns
  start_ns="$(date +%s%N)"
  if "$@" >/dev/null 2>&1; then
    end_ns="$(date +%s%N)"
    echo $(((end_ns - start_ns) / 1000000))
    return 0
  fi
  return 1
}

measure_ms_shell() {
  local cmd="$1"
  local start_ns end_ns
  start_ns="$(date +%s%N)"
  if bash -lc "${cmd}" >/dev/null 2>&1; then
    end_ns="$(date +%s%N)"
    echo $(((end_ns - start_ns) / 1000000))
    return 0
  fi
  return 1
}

percentile() {
  local -n arr_ref=$1
  local p=$2
  local n="${#arr_ref[@]}"
  if [[ "${n}" -eq 0 ]]; then
    echo ""
    return
  fi
  local idx=$(((p * (n - 1) + 99) / 100))
  echo "${arr_ref[$idx]}"
}

emit_profile() {
  local scenario="$1"
  local phase="$2"
  local runtime="$3"
  local samples_csv="$4"
  local error_count="$5"
  local max_p50="$6"
  local max_p95="$7"
  local gate_disabled="$8"

  local -a helper_args=(
    "${PYTHON_BIN}" "${PROFILE_HELPER}"
    "--scenario" "${scenario}"
    "--samples-ms" "${samples_csv}"
    "--error-count" "${error_count}"
    "--label" "phase=${phase}"
    "--label" "transport=stdio"
    "--label" "runtime=${runtime}"
  )

  if [[ "${gate_disabled}" == "1" ]]; then
    helper_args+=("--gate-disabled")
  else
    helper_args+=("--max-p50-ms" "${max_p50}" "--max-p95-ms" "${max_p95}")
  fi

  local profile_json
  set +e
  profile_json="$("${helper_args[@]}")"
  local helper_status=$?
  set -e

  if [[ -z "${profile_json}" ]]; then
    profile_json='{"profile_version":"v1","scenario":"unknown","gate_pass":false}'
    helper_status=2
  fi

  echo "${profile_json}" >>"${SCENARIO_NDJSON}"
  return "${helper_status}"
}

overall_fail=0

run_scenario() {
  local scenario="$1"
  local phase="$2"
  local requires_db="$3"
  local max_p50="$4"
  local max_p95="$5"
  local gate_disabled="$6"
  local runtime="$7"
  local mode="$8"
  shift 8

  local shell_cmd=""
  if [[ "${mode}" == "shell" ]]; then
    shell_cmd="$1"
  fi

  local db_missing=0
  if [[ "${requires_db}" == "1" && -z "${DATABASE_URI:-}" ]]; then
    db_missing=1
  fi

  if [[ "${db_missing}" == "1" && "${REQUIRE_DB_SCENARIOS}" == "1" ]]; then
    echo "${scenario}: failed (DATABASE_URI is required for this gate)" >&2
    if ! emit_profile "${scenario}" "${phase}" "${runtime}" "" "${RUNS}" "${max_p50}" "${max_p95}" "0"; then
      overall_fail=1
    fi
    overall_fail=1
    return
  fi

  if [[ "${db_missing}" == "1" && "${REQUIRE_DB_SCENARIOS}" != "1" ]]; then
    echo "${scenario}: skipped (DATABASE_URI is not configured)" >&2
    emit_profile "${scenario}" "${phase}" "${runtime}" "" "0" "${max_p50}" "${max_p95}" "1" || true
    return
  fi

  local -a times=()
  local failures=0
  for _ in $(seq 1 "${RUNS}"); do
    local ms
    if [[ "${mode}" == "argv" ]]; then
      if ms="$(measure_ms_argv "$@")"; then
        times+=("${ms}")
      else
        failures=$((failures + 1))
      fi
    elif ms="$(measure_ms_shell "${shell_cmd}")"; then
      times+=("${ms}")
    else
      failures=$((failures + 1))
    fi
  done

  IFS=$'\n' sorted=($(printf '%s\n' "${times[@]}" | sort -n))
  unset IFS

  local samples_csv
  samples_csv="$(IFS=,; echo "${sorted[*]}")"
  local count="${#sorted[@]}"
  local min max sum avg p50 p95 p99

  if (( count > 0 )); then
    min="${sorted[0]}"
    max="${sorted[$((count - 1))]}"
    sum=0
    for t in "${sorted[@]}"; do
      sum=$((sum + t))
    done
    avg=$((sum / count))
    p50="$(percentile sorted 50)"
    p95="$(percentile sorted 95)"
    p99="$(percentile sorted 99)"
  else
    min="n/a"
    max="n/a"
    avg="n/a"
    p50="n/a"
    p95="n/a"
    p99="n/a"
  fi

  if emit_profile "${scenario}" "${phase}" "${runtime}" "${samples_csv}" "${failures}" "${max_p50}" "${max_p95}" "${gate_disabled}"; then
    gate_status="pass"
  else
    gate_status="fail"
    overall_fail=1
  fi

  echo "${scenario}: min=${min}ms p50=${p50}ms p95=${p95}ms p99=${p99}ms avg=${avg}ms max=${max}ms runs=${RUNS} errors=${failures} gate=${gate_status}"
}

STARTUP_CMD="${STARTUP_CMD:-}"
FIRST_CALL_CMD="${FIRST_CALL_CMD:-}"
STRESSED_PATH_CMD="${STRESSED_PATH_CMD:-}"

require_shell_override_opt_in() {
  if [[ "${ALLOW_SHELL_OVERRIDE}" != "1" ]]; then
    echo "shell command override is disabled by default; set ALLOW_SHELL_OVERRIDE=1 to enable" >&2
    exit 2
  fi
}

if [[ -n "${STARTUP_CMD}" ]]; then
  require_shell_override_opt_in
  run_scenario "startup_print_tools" "startup" "0" "${STARTUP_MAX_P50_MS}" "${STARTUP_MAX_P95_MS}" "0" "rust" "shell" "${STARTUP_CMD}"
else
  run_scenario "startup_print_tools" "startup" "0" "${STARTUP_MAX_P50_MS}" "${STARTUP_MAX_P95_MS}" "0" "rust" "argv" "${BIN_PATH}" "--print-tools"
fi

if [[ -n "${FIRST_CALL_CMD}" ]]; then
  require_shell_override_opt_in
  run_scenario "first_call_sql_probe" "first_call" "1" "${FIRST_CALL_MAX_P50_MS}" "${FIRST_CALL_MAX_P95_MS}" "0" "rust" "shell" "${FIRST_CALL_CMD}"
else
  run_scenario "first_call_sql_probe" "first_call" "1" "${FIRST_CALL_MAX_P50_MS}" "${FIRST_CALL_MAX_P95_MS}" "0" "rust" "argv" "${BIN_PATH}" "--probe-sql" "${FIRST_CALL_SQL}" "--probe-repeat" "1"
fi

if [[ -n "${STRESSED_PATH_CMD}" ]]; then
  require_shell_override_opt_in
  run_scenario "stressed_path_sql_probe" "stressed_path" "1" "${STRESSED_PATH_MAX_P50_MS}" "${STRESSED_PATH_MAX_P95_MS}" "0" "rust" "shell" "${STRESSED_PATH_CMD}"
else
  run_scenario "stressed_path_sql_probe" "stressed_path" "1" "${STRESSED_PATH_MAX_P50_MS}" "${STRESSED_PATH_MAX_P95_MS}" "0" "rust" "argv" "${BIN_PATH}" "--probe-sql" "${STRESSED_PATH_SQL}" "--probe-repeat" "${STRESSED_PROBE_REPEAT}"
fi

if [[ -n "${PYTHON_BASELINE_CMD:-}" ]]; then
  require_shell_override_opt_in
  run_scenario "startup_python_baseline" "startup" "0" "0" "0" "1" "python" "shell" "${PYTHON_BASELINE_CMD}"
fi

BIN_SHA256="$(sha256sum "${BIN_PATH}" | awk '{print $1}')"
HOSTNAME_VALUE="$(hostname)"
KERNEL_VALUE="$(uname -srmo)"
GENERATED_AT_UTC="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
RUSTC_VERSION="$(rustc --version 2>/dev/null || echo unknown)"

"${PYTHON_BIN}" - "${SCENARIO_NDJSON}" "${REPORT_PATH}" "${GENERATED_AT_UTC}" "${HOSTNAME_VALUE}" "${KERNEL_VALUE}" "${BIN_PATH}" "${BIN_SHA256}" "${RUNS}" "${RUSTC_VERSION}" "${REQUIRE_DB_SCENARIOS}" <<'PY'
import json
import pathlib
import sys

scenarios_path = pathlib.Path(sys.argv[1])
report_path = pathlib.Path(sys.argv[2])
generated_at = sys.argv[3]
host = sys.argv[4]
kernel = sys.argv[5]
bin_path = sys.argv[6]
bin_sha = sys.argv[7]
runs = int(sys.argv[8])
rustc_version = sys.argv[9]
require_db = sys.argv[10] == "1"

scenarios = []
for line in scenarios_path.read_text(encoding="utf-8").splitlines():
    line = line.strip()
    if not line:
        continue
    scenarios.append(json.loads(line))

report = {
    "profile_version": "v1",
    "generated_at_utc": generated_at,
    "context": {
        "host": host,
        "kernel": kernel,
        "binary_path": bin_path,
        "binary_sha256": bin_sha,
        "runs": runs,
        "database_uri_configured": bool(__import__("os").environ.get("DATABASE_URI")),
        "require_db_scenarios": require_db,
        "rustc_version": rustc_version,
    },
    "scenarios": scenarios,
}

report_path.parent.mkdir(parents=True, exist_ok=True)
report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
PY

echo "performance gate report: ${REPORT_PATH}"

if (( overall_fail != 0 )); then
  echo "performance gate failed" >&2
  exit 1
fi

echo "performance gate complete"
