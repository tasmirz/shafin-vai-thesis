#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
EXPECTED_MESSAGES="${EXPECTED_MESSAGES:-400}"
EXPECTED_TOPK="${EXPECTED_TOPK:-${QUERIES:-2}}"

cd "$ROOT_DIR"

python3 scripts/monitor.py \
  --once \
  --expect-mqtt "$EXPECTED_MESSAGES" \
  --expect-kafka "$EXPECTED_MESSAGES" \
  --expect-topk "$EXPECTED_TOPK" \
  --expect-finished-jobs 1 \
  --min-ingestion-completeness 1.0 \
  --min-processing-completeness 0.0 \
  --require-no-issues
