#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BOOTSTRAP_TOOLS=0
STRICT_OUTDATED="${STRICT_OUTDATED:-1}"

if [[ -d "${HOME}/.cargo/bin" ]]; then
  export PATH="${HOME}/.cargo/bin:${PATH}"
fi

usage() {
  cat <<'EOF'
Usage: ./scripts/dependency_governance_check.sh [--bootstrap-tools]

Checks:
  1) cargo deny   -> advisory/license/source policy
  2) cargo audit  -> RustSec vulnerabilities
  3) cargo outdated (direct deps) -> semver-compatible stale-risk gate

Env:
  STRICT_OUTDATED=1  Fail if direct dependencies have semver-compatible updates (default)
  STRICT_OUTDATED=0  Report outdated dependencies without failing

Options:
  --bootstrap-tools  Install missing cargo subcommands with `cargo install --locked`
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --bootstrap-tools)
      BOOTSTRAP_TOOLS=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

SCHED_PREFIX=()
if command -v ionice >/dev/null 2>&1; then
  SCHED_PREFIX+=(ionice -c3)
fi
if command -v nice >/dev/null 2>&1; then
  SCHED_PREFIX+=(nice -n 19)
fi

run_cmd() {
  if [[ ${#SCHED_PREFIX[@]} -gt 0 ]]; then
    "${SCHED_PREFIX[@]}" "$@"
  else
    "$@"
  fi
}

ensure_command() {
  local cmd="$1"
  if ! command -v "${cmd}" >/dev/null 2>&1; then
    echo "missing required command: ${cmd}" >&2
    return 1
  fi
  return 0
}

ensure_cargo_subcommand_binary() {
  local binary="$1"
  local crate="$2"
  if command -v "${binary}" >/dev/null 2>&1; then
    return 0
  fi

  if [[ "${BOOTSTRAP_TOOLS}" -eq 1 ]]; then
    echo "installing ${crate} (missing ${binary})..." >&2
    run_cmd cargo install --locked "${crate}"
    return 0
  fi

  echo "missing ${binary}; install with: cargo install --locked ${crate}" >&2
  return 1
}

cd "${ROOT_DIR}"

ensure_command cargo

missing_tools=0
ensure_cargo_subcommand_binary cargo-deny cargo-deny || missing_tools=1
ensure_cargo_subcommand_binary cargo-audit cargo-audit || missing_tools=1
ensure_cargo_subcommand_binary cargo-outdated cargo-outdated || missing_tools=1

if [[ "${missing_tools}" -ne 0 ]]; then
  echo "dependency governance check aborted due to missing tooling" >&2
  echo "tip: rerun with --bootstrap-tools" >&2
  exit 2
fi

echo "[1/3] cargo deny (advisories + licenses + bans + sources)"
run_cmd cargo deny check advisories licenses bans sources

echo "[2/3] cargo audit (RustSec)"
run_cmd cargo audit --deny warnings

echo "[3/3] cargo outdated (direct dependency stale-risk)"
outdated_tmp="$(mktemp)"
run_cmd cargo outdated --root-deps-only --depth 1 --format json >"${outdated_tmp}"

python3 - "${outdated_tmp}" "${STRICT_OUTDATED}" <<'PY'
import json
import sys
from typing import Any

path = sys.argv[1]
strict = sys.argv[2] == "1"

with open(path, "r", encoding="utf-8") as f:
    payload = json.load(f)

deps = payload.get("dependencies", [])
if not isinstance(deps, list):
    print("unexpected cargo outdated JSON payload shape", file=sys.stderr)
    sys.exit(2)

def text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()

compatible_updates = []
major_only_updates = []

for dep in deps:
    if not isinstance(dep, dict):
        continue
    name = text(dep.get("name"))
    project = text(dep.get("project"))
    compat = text(dep.get("compat"))
    latest = text(dep.get("latest"))

    if not name:
        continue

    if compat and compat != "---" and compat != project:
        compatible_updates.append((name, project, compat, latest))
        continue

    if latest and latest != "---" and latest != project and (not compat or compat == "---"):
        major_only_updates.append((name, project, latest))

if compatible_updates:
    print("compatible direct dependency updates available:")
    for name, project, compat, latest in compatible_updates:
        print(f"  - {name}: project={project} compat={compat} latest={latest}")
else:
    print("no semver-compatible direct dependency updates pending")

if major_only_updates:
    print("major-only direct dependency updates (informational):")
    for name, project, latest in major_only_updates:
        print(f"  - {name}: project={project} latest={latest}")

if strict and compatible_updates:
    sys.exit(1)
PY

rm -f "${outdated_tmp}"

echo "dependency governance checks passed"
