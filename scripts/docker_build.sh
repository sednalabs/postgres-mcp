#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
IMAGE_TAG="${IMAGE_TAG:-postgres-mcp:local}"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required" >&2
  exit 1
fi

if ! docker buildx version >/dev/null 2>&1; then
  echo "docker buildx is required for named build contexts" >&2
  exit 1
fi

SCHED_PREFIX=()
if command -v ionice >/dev/null 2>&1; then
  SCHED_PREFIX+=(ionice -c3)
fi
if command -v nice >/dev/null 2>&1; then
  SCHED_PREFIX+=(nice -n 19)
fi

"${SCHED_PREFIX[@]}" docker buildx build \
  --load \
  --tag "${IMAGE_TAG}" \
  --file "${ROOT_DIR}/Dockerfile" \
  "${ROOT_DIR}"
