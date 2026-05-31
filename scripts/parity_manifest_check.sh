#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MANIFEST="${ROOT_DIR}/fixtures/parity_v2/manifest.json"

if [[ ! -f "${MANIFEST}" ]]; then
  echo "missing manifest: ${MANIFEST}" >&2
  exit 1
fi

python3 - "${ROOT_DIR}" <<'PY'
import json
from pathlib import Path
import sys

root = Path(sys.argv[1]).resolve()
manifest_path = root / "fixtures" / "parity_v2" / "manifest.json"

required_tools = {
    "list_schemas",
    "list_objects",
    "get_object_details",
    "explain_query",
    "execute_sql",
    "analyze_workload_indexes",
    "analyze_query_indexes",
    "analyze_db_health",
    "get_top_queries",
}

with manifest_path.open("r", encoding="utf-8") as f:
    manifest = json.load(f)

tool_set = set(manifest.get("tools", []))
missing_tools = sorted(required_tools - tool_set)
unexpected_tools = sorted(tool_set - required_tools)

if missing_tools:
    raise SystemExit(f"manifest missing tools: {', '.join(missing_tools)}")
if unexpected_tools:
    raise SystemExit(f"manifest has unexpected tools: {', '.join(unexpected_tools)}")

for key in ("fixtures_file", "normalization_rules_file", "known_differences_file"):
    rel = manifest.get(key)
    if not rel:
        raise SystemExit(f"manifest missing key: {key}")
    target = root / rel
    if not target.exists():
        raise SystemExit(f"manifest path does not exist for {key}: {target}")
    with target.open("r", encoding="utf-8") as f:
        json.load(f)

cases_path = root / manifest["fixtures_file"]
with cases_path.open("r", encoding="utf-8") as f:
    cases_doc = json.load(f)
cases = cases_doc.get("cases", [])
if not cases:
    raise SystemExit("tool_cases.json has no cases")

known_path = root / manifest["known_differences_file"]
with known_path.open("r", encoding="utf-8") as f:
    known_doc = json.load(f)
differences = known_doc.get("differences", [])
if not differences:
    raise SystemExit("known_differences.json has no differences")

required_diff_keys = {
    "id",
    "status",
    "tools",
    "title",
    "summary",
    "impact",
    "workaround",
    "target_resolution",
}
known_ids = set()
for diff in differences:
    missing = sorted(required_diff_keys - set(diff.keys()))
    if missing:
        raise SystemExit(
            f"known difference missing required keys ({', '.join(missing)}): {diff}"
        )
    diff_id = diff["id"]
    if diff_id in known_ids:
        raise SystemExit(f"duplicate known difference id: {diff_id}")
    known_ids.add(diff_id)

seen_by_tool = {tool: 0 for tool in required_tools}
seen_known_ids = set()
for case in cases:
    missing = [k for k in ("id", "tool", "comparison_mode") if k not in case]
    if missing:
        raise SystemExit(f"case missing required keys ({', '.join(missing)}): {case}")

    tool = case.get("tool")
    if tool not in seen_by_tool:
        raise SystemExit(f"case has unknown tool: {tool}")
    seen_by_tool[tool] += 1

    mode = case.get("comparison_mode")
    if mode not in {"equivalent", "known_difference"}:
        raise SystemExit(
            f"case {case['id']} has invalid comparison_mode: {mode}"
        )

    diff_ids = case.get("known_difference_ids", [])
    if not isinstance(diff_ids, list):
        raise SystemExit(
            f"case {case['id']} known_difference_ids must be a list"
        )

    if mode == "known_difference" and not diff_ids:
        raise SystemExit(
            f"case {case['id']} is known_difference but has no known_difference_ids"
        )

    for diff_id in diff_ids:
        if diff_id not in known_ids:
            raise SystemExit(
                f"case {case['id']} references unknown known_difference id: {diff_id}"
            )
        seen_known_ids.add(diff_id)

missing_case_tools = sorted([tool for tool, count in seen_by_tool.items() if count == 0])
if missing_case_tools:
    raise SystemExit(
        f"no fixture cases for tool(s): {', '.join(missing_case_tools)}"
    )

orphaned_diffs = sorted(known_ids - seen_known_ids)
if orphaned_diffs:
    raise SystemExit(
        "known differences not referenced by any case: "
        + ", ".join(orphaned_diffs)
    )

print(
    "parity manifest check passed "
    f"(tools={len(required_tools)}, cases={len(cases)}, known_differences={len(known_ids)})"
)
PY
