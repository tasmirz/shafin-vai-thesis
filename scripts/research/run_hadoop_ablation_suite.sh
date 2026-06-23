#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SUITE_ID="${SUITE_ID:-hadoop-ablation-$(date -u +%Y%m%dT%H%M%SZ)}"
CSV_PATH="${CSV_PATH:-$ROOT_DIR/tests/fixtures/paper/mbr-pruning.csv}"
K="${K:-2}"
PARTITIONS="${PARTITIONS:-2}"
VALIDATE_EXACT="${VALIDATE_EXACT:-true}"
BUILD_IMAGE="${BUILD_IMAGE:-1}"
VARIANTS=(baseline aes-only dscp-only aes-dscp)
RUNS=()

cd "$ROOT_DIR"
for variant in "${VARIANTS[@]}"; do
  run_id="${SUITE_ID}-${variant}"
  RUNS+=("$run_id")
  echo "== Running Hadoop PTD treatment: $variant =="
  RUN_ID="$run_id" ALGORITHM="$variant" CSV_PATH="$CSV_PATH" K="$K" PARTITIONS="$PARTITIONS" \
    VALIDATE_EXACT="$VALIDATE_EXACT" BUILD_IMAGE="$BUILD_IMAGE" \
    scripts/research/run_hadoop_csv_benchmark.sh
  BUILD_IMAGE=0
done

python3 - "$SUITE_ID" <<'PY'
import json
import sys
from pathlib import Path

root = Path("reports/runs")
suite = sys.argv[1]
metrics = {
    variant: json.loads((root / f"{suite}-{variant}" / "metrics.json").read_text())["spark"]
    for variant in ("baseline", "aes-only", "dscp-only", "aes-dscp")
}
baseline = metrics["baseline"]
aes = metrics["aes-only"]
dscp = metrics["dscp-only"]
full = metrics["aes-dscp"]
assert baseline["engine"] == "apache-hadoop"
assert all(result["engine"] == "apache-hadoop" for result in metrics.values())
assert baseline["totalEmittedRecords"] >= aes["totalEmittedRecords"], "AES increased emissions"
assert dscp["totalEmittedRecords"] >= full["totalEmittedRecords"], "AES increased DSCP emissions"
assert baseline["avgPruneRatio"] == aes["avgPruneRatio"], "AES changed candidate pruning"
assert dscp["avgPruneRatio"] == full["avgPruneRatio"], "AES changed DSCP pruning"
assert all(result["falsePruneCount"] == 0 for result in metrics.values()), "false prune detected"
print("hadoopAblationAssertions=passed")
PY

echo "== Hadoop PTD treatment comparison =="
python3 scripts/research/compare_runs.py "${RUNS[@]}"
