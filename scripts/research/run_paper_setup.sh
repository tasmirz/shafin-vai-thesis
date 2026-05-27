#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SETUP="${SETUP:-paired}"
PROFILE="${PROFILE:-smartphone}"
SUITE_ID="${SUITE_ID:-paper-setup-$(date -u +%Y%m%dT%H%M%SZ)}"
BUILD_IMAGE="${BUILD_IMAGE:-1}"
K="${K:-10}"
PARTITIONS="${PARTITIONS:-8}"
VALIDATE_EXACT="${VALIDATE_EXACT:-false}"
SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-2g}"
SPARK_MASTER="${SPARK_MASTER:-local[4]}"
TRACE_LIMIT="${TRACE_LIMIT:-25}"

case "$PROFILE" in
  smartphone)
    CSV_PATH="${CSV_PATH:-$ROOT_DIR/datasets-curated/smartphone-paper.csv}"
    DATASET_MANIFEST="${DATASET_MANIFEST:-$ROOT_DIR/reports/datasets/smartphone-paper.json}"
    PAPER_DATASET="Synthetic smartphone"
    PAPER_BASELINE_MS=66520
    PAPER_UPGRADE_MS=43757
    PAPER_REDUCTION=34.2
    ;;
  road-smoke)
    CSV_PATH="${CSV_PATH:-$ROOT_DIR/datasets-curated/bangladesh-road-smoke.csv}"
    DATASET_MANIFEST="${DATASET_MANIFEST:-$ROOT_DIR/reports/datasets/bangladesh-road-smoke.json}"
    PAPER_DATASET="Bangladesh road / OSM (smoke input)"
    PAPER_BASELINE_MS=56274
    PAPER_UPGRADE_MS=42366
    PAPER_REDUCTION=24.7
    ;;
  road-full)
    CSV_PATH="${CSV_PATH:-$ROOT_DIR/datasets-curated/bangladesh-road-paper.csv}"
    DATASET_MANIFEST="${DATASET_MANIFEST:-$ROOT_DIR/reports/datasets/bangladesh-road-paper.json}"
    PAPER_DATASET="Bangladesh road / OSM"
    PAPER_BASELINE_MS=56274
    PAPER_UPGRADE_MS=42366
    PAPER_REDUCTION=24.7
    if [[ "${ALLOW_FULL_ROAD:-false}" != "true" ]]; then
      echo "road-full requires ALLOW_FULL_ROAD=true because it executes 98,451 MBR objects." >&2
      exit 2
    fi
    ;;
  road-full-20q)
    CSV_PATH="${CSV_PATH:-$ROOT_DIR/datasets-curated/bangladesh-road-paper.csv}"
    DATASET_MANIFEST="${DATASET_MANIFEST:-$ROOT_DIR/reports/datasets/bangladesh-road-paper.json}"
    QUERY_SET_PATH="${QUERY_SET_PATH:-$ROOT_DIR/datasets-curated/bangladesh-road-queries-20.csv}"
    QUERY_SET_MANIFEST="${QUERY_SET_MANIFEST:-$ROOT_DIR/reports/datasets/bangladesh-road-queries-20.json}"
    PAPER_DATASET="Bangladesh road / OSM (fixed 20-query protocol)"
    PAPER_BASELINE_MS=56274
    PAPER_UPGRADE_MS=42366
    PAPER_REDUCTION=24.7
    if [[ "${ALLOW_FULL_ROAD:-false}" != "true" ]]; then
      echo "road-full-20q requires ALLOW_FULL_ROAD=true because it executes 98,451 MBR objects over 20 queries." >&2
      exit 2
    fi
    ;;
  *)
    echo "Unknown PROFILE '$PROFILE'; use smartphone, road-smoke, road-full or road-full-20q." >&2
    exit 1
    ;;
esac

cd "$ROOT_DIR"
test -f "$CSV_PATH"
test -f "$DATASET_MANIFEST"
if [[ -n "${QUERY_SET_PATH:-}" ]]; then
  test -f "$QUERY_SET_PATH"
  test -f "$QUERY_SET_MANIFEST"
fi

run_treatment() {
  local suffix="$1"
  local algorithm="$2"
  local run_id="${SUITE_ID}-${suffix}"
  if [[ "${REUSE_EXISTING_RUNS:-false}" == "true" && -f "$ROOT_DIR/reports/runs/$run_id/metrics.json" ]]; then
    echo "Reusing saved treatment: $run_id"
    return
  fi
  RUN_ID="$run_id" ALGORITHM="$algorithm" CSV_PATH="$CSV_PATH" \
    DATASET_MANIFEST="$DATASET_MANIFEST" QUERY_SET_PATH="${QUERY_SET_PATH:-}" \
    QUERY_SET_MANIFEST="${QUERY_SET_MANIFEST:-}" K="$K" PARTITIONS="$PARTITIONS" \
    VALIDATE_EXACT="$VALIDATE_EXACT" RUN_LOCAL_ORACLE="$VALIDATE_EXACT" \
    SETUP_FAMILY="rai-lian-iccit-${SETUP}" SETUP_ROLE="$suffix" \
    BUILD_IMAGE="$BUILD_IMAGE" SPARK_DRIVER_MEMORY="$SPARK_DRIVER_MEMORY" \
    SPARK_MASTER="$SPARK_MASTER" TRACE_LIMIT="$TRACE_LIMIT" \
    scripts/research/run_csv_benchmark.sh
  BUILD_IMAGE=0
}

case "$SETUP" in
  rai-baseline)
    echo "Executing Rai-Lian-style Spark indexed baseline setup without AES or DSCP."
    run_treatment "rai-lian-baseline" "baseline"
    ;;
  iccit-upgrade)
    echo "Executing Spark ICCIT upgrade setup with AES and DSCP enabled."
    run_treatment "iccit-aes-dscp" "aes-dscp"
    ;;
  paired)
    echo "Executing paired Rai-Lian baseline versus Spark ICCIT AES+DSCP setup."
    run_treatment "rai-lian-baseline" "baseline"
    run_treatment "iccit-aes-dscp" "aes-dscp"
    python3 - "$SUITE_ID" "$PAPER_DATASET" "$PAPER_BASELINE_MS" "$PAPER_UPGRADE_MS" "$PAPER_REDUCTION" <<'PY'
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

suite, dataset, paper_baseline, paper_upgrade, paper_reduction = sys.argv[1:]
base = json.loads(Path(f"reports/runs/{suite}-rai-lian-baseline/metrics.json").read_text())["spark"]
upgrade = json.loads(Path(f"reports/runs/{suite}-iccit-aes-dscp/metrics.json").read_text())["spark"]
reduction = (base["algorithmElapsedMs"] - upgrade["algorithmElapsedMs"]) / base["algorithmElapsedMs"] * 100
content = f"""# Paired Paper Setup Comparison: `{suite}`

Generated: {datetime.now(timezone.utc).isoformat()}

| Dataset | Setup | Runtime | Status |
|---|---|---:|---|
| {dataset} | Published ICCIT Hadoop baseline | {int(paper_baseline):,} ms | reference only |
| {dataset} | Published ICCIT Hadoop AES+DSCP | {int(paper_upgrade):,} ms | reference only ({paper_reduction}% published reduction) |
| {dataset} | Spark Rai-Lian indexed baseline (`baseline`) | {base['algorithmElapsedMs']:,} ms | observed current-machine control |
| {dataset} | Spark ICCIT AES+DSCP (`aes-dscp`) | {upgrade['algorithmElapsedMs']:,} ms | observed current-machine upgrade |

Within-Spark reduction against the Rai-Lian indexed baseline: **{reduction:.2f}%**.

Published Hadoop milliseconds are not combined with Spark timings as an engine speedup because
the hardware and runtime conditions differ.
"""
output = Path(f"reports/validation/{suite}-paired.md")
output.write_text(content)
print(f"pairedComparisonReport={output}")
PY
    ;;
  ablation)
    PROFILE="$PROFILE" SUITE_ID="$SUITE_ID" CSV_PATH="$CSV_PATH" \
      DATASET_MANIFEST="$DATASET_MANIFEST" QUERY_SET_PATH="${QUERY_SET_PATH:-}" \
      QUERY_SET_MANIFEST="${QUERY_SET_MANIFEST:-}" K="$K" PARTITIONS="$PARTITIONS" \
      VALIDATE_EXACT="$VALIDATE_EXACT" BUILD_IMAGE="$BUILD_IMAGE" \
      SPARK_DRIVER_MEMORY="$SPARK_DRIVER_MEMORY" SPARK_MASTER="$SPARK_MASTER" \
      TRACE_LIMIT="$TRACE_LIMIT" ALLOW_FULL_ROAD="${ALLOW_FULL_ROAD:-false}" \
      scripts/research/run_iccit_comparison_suite.sh
    ;;
  *)
    echo "Unknown SETUP '$SETUP'; use rai-baseline, iccit-upgrade, paired or ablation." >&2
    exit 1
    ;;
esac
