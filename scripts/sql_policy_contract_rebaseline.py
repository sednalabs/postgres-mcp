#!/usr/bin/env python3
"""Rebaseline SQL restricted-policy contract alignment and hashes."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


CODE_RE = re.compile(r'Self::[A-Za-z0-9_]+\s*=>\s*"([A-Z0-9_]+)"')


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def maybe_git_head(path: Path) -> str | None:
    try:
        proc = subprocess.run(
            ["git", "-C", str(path), "rev-parse", "HEAD"],
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError:
        return None
    if proc.returncode != 0:
        return None
    return proc.stdout.strip() or None


def extract_local_codes(sql_read_only_rs: Path) -> list[str]:
    text = sql_read_only_rs.read_text(encoding="utf-8")
    codes: list[str] = []
    for match in CODE_RE.finditer(text):
        code = match.group(1)
        if code not in codes:
            codes.append(code)
    if not codes:
        raise ValueError(f"no classifier codes found in {sql_read_only_rs}")
    return codes


def load_contract_codes(contract_json_path: Path) -> tuple[str, str, list[str]]:
    payload = json.loads(contract_json_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("contract payload must be a JSON object")
    version = payload.get("policy_contract_version")
    contract_name = payload.get("contract_name")
    if not isinstance(version, str) or not version:
        raise ValueError("contract missing policy_contract_version")
    if not isinstance(contract_name, str) or not contract_name:
        raise ValueError("contract missing contract_name")
    classifier = payload.get("classifier")
    if not isinstance(classifier, dict):
        raise ValueError("contract missing classifier object")
    code_entries = classifier.get("codes")
    if not isinstance(code_entries, list) or not code_entries:
        raise ValueError("contract classifier.codes must be a non-empty list")
    codes: list[str] = []
    for item in code_entries:
        if not isinstance(item, dict) or not isinstance(item.get("code"), str):
            raise ValueError("contract classifier code entries must be objects with code")
        code = item["code"]
        if code in codes:
            raise ValueError(f"duplicate contract code: {code}")
        codes.append(code)
    return contract_name, version, codes


def load_registry_contract(
    registry_json_path: Path, expected_contract_name: str
) -> tuple[str, str]:
    payload = json.loads(registry_json_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("registry payload must be a JSON object")

    contracts = payload.get("contracts")
    if not isinstance(contracts, list) or not contracts:
        raise ValueError("registry contracts must be a non-empty list")

    for item in contracts:
        if not isinstance(item, dict):
            continue
        if item.get("contract_name") != expected_contract_name:
            continue

        version = item.get("policy_contract_version")
        digest = item.get("generated_json_sha256")
        if not isinstance(version, str) or not version:
            raise ValueError(
                f"registry entry for {expected_contract_name} missing policy_contract_version"
            )
        if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
            raise ValueError(
                f"registry entry for {expected_contract_name} missing generated_json_sha256"
            )
        return version, digest

    raise ValueError(
        f"registry does not contain contract entry for {expected_contract_name}"
    )


def render_report(
    *,
    contract_path: Path,
    policy_source_path: Path,
    contract_name: str,
    contract_version: str,
    registry_path: Path,
    registry_contract_version: str,
    registry_contract_sha256: str,
    contract_codes: list[str],
    local_codes: list[str],
    kernel_root: Path,
    toolkit_root: Path,
) -> dict[str, Any]:
    contract_code_set = set(contract_codes)
    local_code_set = set(local_codes)
    missing_in_local = sorted(contract_code_set - local_code_set)
    extra_in_local = sorted(local_code_set - contract_code_set)
    order_match = contract_codes == local_codes
    codes_match = not missing_in_local and not extra_in_local and order_match

    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "contract": {
            "path": str(contract_path),
            "name": contract_name,
            "policy_contract_version": contract_version,
            "sha256": sha256_file(contract_path),
            "codes": contract_codes,
        },
        "registry_contract": {
            "path": str(registry_path),
            "policy_contract_version": registry_contract_version,
            "generated_json_sha256": registry_contract_sha256,
        },
        "local_classifier": {
            "path": str(policy_source_path),
            "sha256": sha256_file(policy_source_path),
            "codes": local_codes,
        },
        "comparison": {
            "codes_match": codes_match,
            "order_match": order_match,
            "missing_in_local": missing_in_local,
            "extra_in_local": extra_in_local,
        },
        "git": {
            "kernel_head": maybe_git_head(kernel_root),
            "toolkit_head": maybe_git_head(toolkit_root),
        },
    }


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(
        description=(
            "Rebaseline SQL restricted-policy contract alignment between "
            "mcp-policy-kernel and mcp-toolkit-policy-core."
        )
    )
    parser.add_argument(
        "--kernel-root",
        type=Path,
        default=(root / "../mcp-policy-kernel").resolve(),
        help="Path to mcp-policy-kernel repo.",
    )
    parser.add_argument(
        "--toolkit-root",
        type=Path,
        default=(root / "../mcp-toolkit-rs").resolve(),
        help="Path to mcp-toolkit-rs repo.",
    )
    parser.add_argument(
        "--contract-json",
        type=Path,
        default=None,
        help="Override contract artifact path.",
    )
    parser.add_argument(
        "--registry-json",
        type=Path,
        default=None,
        help="Override policy contract registry artifact path.",
    )
    parser.add_argument(
        "--policy-source",
        type=Path,
        default=None,
        help="Override policy-core sql_read_only.rs path.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=(root / ".tmp/policy_contract_rebaseline/sql_policy_contract_rebaseline.json"),
        help="Output report path.",
    )
    args = parser.parse_args()

    contract_path = (
        args.contract_json
        if args.contract_json is not None
        else args.kernel_root / "spec/generated/sql_restricted_policy_contract.v1.json"
    )
    registry_path = (
        args.registry_json
        if args.registry_json is not None
        else args.kernel_root / "spec/generated/policy_contract_registry.v1.json"
    )
    policy_source = (
        args.policy_source
        if args.policy_source is not None
        else args.toolkit_root
        / "crates/mcp-toolkit-policy-core/src/sql_read_only.rs"
    )

    if not contract_path.exists():
        print(f"missing contract artifact: {contract_path}", file=sys.stderr)
        print(
            "run: python3 mcp-policy-kernel/scripts/sync_sql_restricted_policy_contract.py",
            file=sys.stderr,
        )
        return 2
    if not registry_path.exists():
        print(f"missing registry artifact: {registry_path}", file=sys.stderr)
        print(
            "run: python3 mcp-policy-kernel/scripts/sync_policy_contract_registry.py",
            file=sys.stderr,
        )
        return 2
    if not policy_source.exists():
        print(f"missing policy source: {policy_source}", file=sys.stderr)
        return 2

    contract_name, contract_version, contract_codes = load_contract_codes(contract_path)
    registry_contract_version, registry_contract_sha256 = load_registry_contract(
        registry_path, contract_name
    )
    contract_sha256 = sha256_file(contract_path)
    if registry_contract_version != contract_version:
        print(
            (
                "registry/version mismatch: "
                f"contract version={contract_version}, "
                f"registry version={registry_contract_version}"
            ),
            file=sys.stderr,
        )
        return 1
    if registry_contract_sha256 != contract_sha256:
        print(
            (
                "registry/hash mismatch: "
                f"contract sha256={contract_sha256}, "
                f"registry sha256={registry_contract_sha256}"
            ),
            file=sys.stderr,
        )
        return 1

    local_codes = extract_local_codes(policy_source)
    report = render_report(
        contract_path=contract_path,
        policy_source_path=policy_source,
        contract_name=contract_name,
        contract_version=contract_version,
        registry_path=registry_path,
        registry_contract_version=registry_contract_version,
        registry_contract_sha256=registry_contract_sha256,
        contract_codes=contract_codes,
        local_codes=local_codes,
        kernel_root=args.kernel_root,
        toolkit_root=args.toolkit_root,
    )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"wrote rebaseline report: {args.output}")

    if not report["comparison"]["codes_match"]:
        print("contract/code mismatch detected:", file=sys.stderr)
        print(json.dumps(report["comparison"], indent=2, sort_keys=True), file=sys.stderr)
        return 1

    print(
        "sql restricted-policy contract alignment ok "
        f"(version={contract_version}, codes={len(contract_codes)})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
