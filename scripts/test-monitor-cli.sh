#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
EXPECTED_MESSAGES="${EXPECTED_MESSAGES:-400}"

cd "$ROOT_DIR"

python3 scripts/monitor.py \
  --once \
  --expect-mqtt "$EXPECTED_MESSAGES" \
  --expect-kafka "$EXPECTED_MESSAGES" \
  --expect-topk "$EXPECTED_MESSAGES" \
  --expect-finished-jobs 1 \
  --min-ingestion-completeness 1.0 \
  --min-processing-completeness 1.0 \
  --require-no-issues
