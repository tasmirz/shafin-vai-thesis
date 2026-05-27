#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SUITE_ID="${SUITE_ID:-ablation-$(date -u +%Y%m%dT%H%M%SZ)}"
BUILD_IMAGE="${BUILD_IMAGE:-1}"
CSV_PATH="${CSV_PATH:-$ROOT_DIR/tests/fixtures/paper/mbr-pruning.csv}"
DATASET_MANIFEST="${DATASET_MANIFEST:-}"
QUERY_SET_PATH="${QUERY_SET_PATH:-}"
QUERY_SET_MANIFEST="${QUERY_SET_MANIFEST:-}"
K="${K:-2}"
PARTITIONS="${PARTITIONS:-2}"
VALIDATE_EXACT="${VALIDATE_EXACT:-true}"
REQUIRE_EXACT="${REQUIRE_EXACT:-$VALIDATE_EXACT}"
REQUIRE_PRUNING="${REQUIRE_PRUNING:-true}"
REUSE_EXISTING_RUNS="${REUSE_EXISTING_RUNS:-false}"
SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-2g}"
SPARK_MASTER="${SPARK_MASTER:-local[4]}"
TRACE_LIMIT="${TRACE_LIMIT:-25}"
VARIANTS=(baseline dscp-only aes-only aes-dscp)
RUNS=()

cd "$ROOT_DIR"
if [[ ! "$SUITE_ID" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]]; then
  echo "Invalid SUITE_ID: use letters, digits, '.', '_' and '-' only" >&2
  exit 1
fi

for variant in "${VARIANTS[@]}"; do
  run_id="${SUITE_ID}-${variant}"
  RUNS+=("$run_id")
  if [[ "$REUSE_EXISTING_RUNS" == "true" && -f "$ROOT_DIR/reports/runs/$run_id/metrics.json" ]]; then
    echo "== Reusing completed PTD treatment: $variant =="
    continue
  fi
  echo "== Running PTD treatment: $variant =="
  RUN_ID="$run_id" ALGORITHM="$variant" CSV_PATH="$CSV_PATH" \
    DATASET_MANIFEST="$DATASET_MANIFEST" QUERY_SET_PATH="$QUERY_SET_PATH" \
    QUERY_SET_MANIFEST="$QUERY_SET_MANIFEST" K="$K" PARTITIONS="$PARTITIONS" \
    VALIDATE_EXACT="$VALIDATE_EXACT" RUN_LOCAL_ORACLE="$VALIDATE_EXACT" BUILD_IMAGE="$BUILD_IMAGE" \
    SPARK_DRIVER_MEMORY="$SPARK_DRIVER_MEMORY" \
    SPARK_MASTER="$SPARK_MASTER" \
    TRACE_LIMIT="$TRACE_LIMIT" \
    scripts/research/run_csv_benchmark.sh
  BUILD_IMAGE=0
done

python3 - "$SUITE_ID" "$REQUIRE_EXACT" "$REQUIRE_PRUNING" <<'PY'
import json
import sys
from pathlib import Path

root = Path("reports/runs")
suite = sys.argv[1]
metrics = {
    variant: json.loads((root / f"{suite}-{variant}" / "metrics.json").read_text())["spark"]
    for variant in ("baseline", "dscp-only", "aes-only", "aes-dscp")
}
baseline = metrics["baseline"]
dscp = metrics["dscp-only"]
aes = metrics["aes-only"]
full = metrics["aes-dscp"]

indexed = all(
    result.get("boundMode") == "rai-lian-artree-selected-level-partial-reducer"
    for result in metrics.values()
)
if indexed:
    assert baseline["totalEmittedRecords"] >= aes["totalEmittedRecords"], "AES increased emissions"
    assert dscp["totalEmittedRecords"] >= full["totalEmittedRecords"], "AES increased DSCP emissions"
    assert baseline["avgPruneRatio"] == aes["avgPruneRatio"], (
        "AES changed Rai-Lian baseline candidate selection")
    assert dscp["avgPruneRatio"] >= baseline["avgPruneRatio"], (
        "DSCP retained more candidates than the indexed baseline")
else:
    assert baseline["totalEmittedRecords"] > aes["totalEmittedRecords"], "AES did not reduce emissions"
    assert dscp["totalEmittedRecords"] > full["totalEmittedRecords"], "AES did not reduce DSCP emissions"
    assert baseline["avgPruneRatio"] == 0 and aes["avgPruneRatio"] == 0, "non-DSCP treatment pruned"
assert dscp["avgPruneRatio"] == full["avgPruneRatio"], "AES changed DSCP candidate selection"
assert dscp["totalBaselineEmissions"] == full["totalBaselineEmissions"], "AES changed DSCP baseline scope"
if sys.argv[2].lower() in ("1", "true"):
    assert all(result["falsePruneCount"] == 0 for result in metrics.values()), (
        "indexed or DSCP treatment false prune detected")
if sys.argv[3].lower() in ("1", "true") and all(result.get("boundMode") in {
        "ddr-mbr-full-possible",
        "rai-lian-artree-selected-level-partial-reducer",
    } for result in metrics.values()):
    assert dscp["avgPruneRatio"] > 0, "MBR-backed DSCP did not certify any pruning"
assert all(
    result.get("boundMode") in {
        "partition-local-conservative-no-mbr",
        "mbr-when-present-otherwise-conservative",
        "conservative-remote-mass-no-mbr",
        "ddr-mbr-full-possible",
        "rai-lian-artree-selected-level-partial-reducer",
    }
    and result.get("emissionScope") == "server-partition"
    for result in metrics.values()
), "run lacks paper-alignment mode evidence"
print("ablationAssertions=passed emissionChecks=true exactValidation=" + sys.argv[2])
PY

echo "== Controlled ablation comparison =="
python3 scripts/research/compare_runs.py "${RUNS[@]}"
