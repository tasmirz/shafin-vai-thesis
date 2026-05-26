#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

SUITE_ID="${SUITE_ID:-csv-ablation-test-$(date -u +%Y%m%dT%H%M%SZ)-$$}" \
  BUILD_IMAGE="${BUILD_IMAGE:-0}" \
  K="${K:-2}" \
  PARTITIONS="${PARTITIONS:-2}" \
  scripts/research/run_ablation_suite.sh
