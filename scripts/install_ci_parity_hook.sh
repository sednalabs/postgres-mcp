#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

HOOKS_DIR="$(git rev-parse --git-path hooks)"
HOOK_PATH="${HOOKS_DIR}/pre-push"
TMP_PATH="${HOOKS_DIR}/.pre-push.ci-parity.tmp"
START_MARKER="# >>> postgres-mcp ci parity advisory >>>"
END_MARKER="# <<< postgres-mcp ci parity advisory <<<"

if [ -z "${HOOKS_DIR}" ]; then
  echo "error: unable to determine git hooks directory; run from repository root." >&2
  exit 1
fi

mkdir -p "${HOOKS_DIR}"

SNIPPET="$(cat <<'EOF'
# >>> postgres-mcp ci parity advisory >>>
if [ -x "./scripts/ci_parity_advisory.sh" ]; then
  if ! ./scripts/ci_parity_advisory.sh; then
    echo "[advisory] ci_parity_advisory.sh reported issues; continuing push (advisory mode)." >&2
  fi
else
  echo "[advisory] scripts/ci_parity_advisory.sh missing or not executable; skipping." >&2
fi
# <<< postgres-mcp ci parity advisory <<<
EOF
)"

if [ ! -f "${HOOK_PATH}" ]; then
  cat > "${HOOK_PATH}" <<EOF
#!/usr/bin/env bash
set -euo pipefail

${SNIPPET}
EOF
  chmod +x "${HOOK_PATH}"
  echo "installed advisory pre-push hook at ${HOOK_PATH}"
  exit 0
fi

if grep -Fq "${START_MARKER}" "${HOOK_PATH}"; then
  if ! grep -Fq "${END_MARKER}" "${HOOK_PATH}"; then
    echo "warning: found advisory start marker without end marker in ${HOOK_PATH}; leaving hook unchanged." >&2
    exit 0
  fi
  awk -v start="${START_MARKER}" -v end="${END_MARKER}" '
    $0 == start { in_block=1; next }
    $0 == end { in_block=0; next }
    !in_block { print }
  ' "${HOOK_PATH}" > "${TMP_PATH}"
  mv "${TMP_PATH}" "${HOOK_PATH}"
  chmod +x "${HOOK_PATH}"
fi

awk -v snippet="${SNIPPET}" '
  BEGIN { inserted = 0 }
  {
    line = $0
    sub(/[[:space:]]*#.*/, "", line)
    sub(/[[:space:]]*;[[:space:]]*$/, "", line)
    if (!inserted && line ~ /^[[:space:]]*exit([[:space:]]+.*)?[[:space:]]*$/) {
      print ""
      print snippet
      inserted = 1
    }
    print
  }
  END {
    if (!inserted) {
      print ""
      print snippet
    }
  }
' "${HOOK_PATH}" > "${TMP_PATH}"
mv "${TMP_PATH}" "${HOOK_PATH}"
chmod +x "${HOOK_PATH}"
echo "updated advisory pre-push hook at ${HOOK_PATH}"
