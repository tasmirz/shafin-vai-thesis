#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SUITE_ID="${SUITE_ID:-ablation-$(date -u +%Y%m%dT%H%M%SZ)}"
BUILD_IMAGE="${BUILD_IMAGE:-1}"
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
  echo "== Running PTD treatment: $variant =="
  RUN_ID="$run_id" ALGORITHM="$variant" BUILD_IMAGE="$BUILD_IMAGE" \
    scripts/research/run_csv_benchmark.sh
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
    for variant in ("baseline", "dscp-only", "aes-only", "aes-dscp")
}
baseline = metrics["baseline"]
dscp = metrics["dscp-only"]
aes = metrics["aes-only"]
full = metrics["aes-dscp"]

assert baseline["totalEmittedRecords"] > aes["totalEmittedRecords"], "AES did not reduce emissions"
assert dscp["totalEmittedRecords"] > full["totalEmittedRecords"], "AES did not reduce DSCP emissions"
assert baseline["avgPruneRatio"] == 0 and aes["avgPruneRatio"] == 0, "non-DSCP treatment pruned"
assert dscp["avgPruneRatio"] == full["avgPruneRatio"], "AES changed DSCP candidate selection"
assert dscp["totalBaselineEmissions"] == full["totalBaselineEmissions"], "AES changed DSCP baseline scope"
assert dscp["falsePruneCount"] == 0 and full["falsePruneCount"] == 0, "DSCP false prune detected"
assert all(
    result.get("boundMode") == "partition-local-conservative-no-mbr"
    and result.get("emissionScope") == "server-partition"
    for result in metrics.values()
), "run lacks paper-alignment mode evidence"
print("ablationAssertions=passed emissionsAndFalsePruneChecks=true pruningMayRequireMbr=true")
PY

echo "== Controlled ablation comparison =="
python3 scripts/research/compare_runs.py "${RUNS[@]}"
