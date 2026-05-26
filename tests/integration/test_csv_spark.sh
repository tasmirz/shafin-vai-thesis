#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"
RUN_ID="${RUN_ID:-csv-test-$(date -u +%Y%m%dT%H%M%SZ)-$$}" \
  CSV_PATH="$ROOT_DIR/tests/fixtures/csv/smartphone-small.csv" \
  K=2 PARTITIONS=2 SEED=42 \
  scripts/research/run_csv_benchmark.sh
