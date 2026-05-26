#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"
objects="${OBJECTS:-12}"
queries="${QUERIES:-2}"
run_id="${RUN_ID:-stream-test-$(date -u +%Y%m%dT%H%M%SZ)-$$}"
report_dir="${E2E_REPORT_DIR:-$ROOT_DIR/reports/e2e/test-runs/$run_id}"
RUN_ID="$run_id" E2E_REPORT_DIR="$report_dir" \
OBJECTS="$objects" QUERIES="$queries" DIMENSIONS="${DIMENSIONS:-2}" \
K="${K:-2}" PARTITIONS="${PARTITIONS:-2}" EXPECTED_MESSAGES="${EXPECTED_MESSAGES:-$((objects * queries))}" \
RATE_PER_SECOND="${RATE_PER_SECOND:-100}" BUILD_IMAGE="${BUILD_IMAGE:-1}" \
scripts/e2e-benchmark.sh
grep -q "reader=structured-streaming trigger=available-now" "$report_dir/spark.log"
