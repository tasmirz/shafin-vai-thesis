#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SUITE_ID="${SUITE_ID:-iccit-spark-$(date -u +%Y%m%dT%H%M%SZ)}"
PROFILE="${PROFILE:-smartphone}"
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
    PAPER_BASELINE_MS=66520
    PAPER_PROPOSED_MS=43757
    PAPER_REDUCTION=34.2
    ;;
  road-smoke)
    CSV_PATH="${CSV_PATH:-$ROOT_DIR/datasets-curated/bangladesh-road-smoke.csv}"
    DATASET_MANIFEST="${DATASET_MANIFEST:-$ROOT_DIR/reports/datasets/bangladesh-road-smoke.json}"
    PAPER_BASELINE_MS=56274
    PAPER_PROPOSED_MS=42366
    PAPER_REDUCTION=24.7
    ;;
  road-full)
    CSV_PATH="${CSV_PATH:-$ROOT_DIR/datasets-curated/bangladesh-road-paper.csv}"
    DATASET_MANIFEST="${DATASET_MANIFEST:-$ROOT_DIR/reports/datasets/bangladesh-road-paper.json}"
    PAPER_BASELINE_MS=56274
    PAPER_PROPOSED_MS=42366
    PAPER_REDUCTION=24.7
    if [[ "${ALLOW_FULL_ROAD:-false}" != "true" ]]; then
      echo "road-full is intentionally opt-in: it executes the 98,451-object aggregate-R-tree" >&2
      echo "path and can be expensive. Set ALLOW_FULL_ROAD=true to run it." >&2
      echo "Use PROFILE=road-smoke for a fast exact indexed-path validation." >&2
      exit 2
    fi
    ;;
  *)
    echo "Unknown PROFILE '$PROFILE'; use smartphone, road-smoke or road-full." >&2
    exit 1
    ;;
esac

cd "$ROOT_DIR"
test -f "$CSV_PATH"
test -f "$DATASET_MANIFEST"

echo "Running same-machine ICCIT treatment comparison."
echo "profile=$PROFILE k=$K partitions=$PARTITIONS validateExact=$VALIDATE_EXACT"
echo "sparkDriverMemory=$SPARK_DRIVER_MEMORY"
echo "sparkMaster=$SPARK_MASTER"
echo "traceLimit=$TRACE_LIMIT"
echo "The ICCIT paper does not publish k, partition count or query seeds; these are declared protocol assumptions."

SUITE_ID="$SUITE_ID" CSV_PATH="$CSV_PATH" DATASET_MANIFEST="$DATASET_MANIFEST" \
  K="$K" PARTITIONS="$PARTITIONS" VALIDATE_EXACT="$VALIDATE_EXACT" \
  REQUIRE_EXACT="$VALIDATE_EXACT" REQUIRE_PRUNING=false \
  REUSE_EXISTING_RUNS="${REUSE_EXISTING_RUNS:-false}" BUILD_IMAGE="$BUILD_IMAGE" \
  SPARK_DRIVER_MEMORY="$SPARK_DRIVER_MEMORY" \
  SPARK_MASTER="$SPARK_MASTER" \
  TRACE_LIMIT="$TRACE_LIMIT" \
  scripts/research/run_ablation_suite.sh

OUTPUT="$ROOT_DIR/reports/validation/${SUITE_ID}.md"
python3 - "$SUITE_ID" "$PROFILE" "$K" "$PARTITIONS" "$VALIDATE_EXACT" "$SPARK_DRIVER_MEMORY" "$SPARK_MASTER" \
  "$PAPER_BASELINE_MS" "$PAPER_PROPOSED_MS" "$PAPER_REDUCTION" "$OUTPUT" <<'PY'
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

suite, profile, k, partitions, validated, driver_memory, spark_master, paper_base, paper_full, paper_reduction, output = sys.argv[1:]
root = Path("reports/runs")
variants = ("baseline", "aes-only", "dscp-only", "aes-dscp")
metrics = {
    variant: json.loads((root / f"{suite}-{variant}" / "metrics.json").read_text())
    for variant in variants
}
baseline = metrics["baseline"]["spark"]["algorithmElapsedMs"]

def reduction(elapsed):
  return ((baseline - elapsed) / baseline * 100.0) if baseline else 0.0

rows = []
for variant in variants:
  spark = metrics[variant]["spark"]
  exact = metrics[variant]["validation"]["exactTopKAgreement"]
  prune = f"{spark['avgPruneRatio'] * 100:.2f}%"
  rows.append(
      f"| {variant} | {spark['algorithmElapsedMs']:,} | {reduction(spark['algorithmElapsedMs']):.2f}% | "
      f"{prune} | {spark['totalEmittedRecords']:,} | {spark['totalShuffleBytes']:,} | "
      f"{spark.get('totalPartialMbrRefs', 0):,} | {exact if exact is not None else 'not run'} |")

content = f"""# Same-Machine ICCIT/Spark Comparison: `{suite}`

Generated: {datetime.now(timezone.utc).isoformat()}

## Interpretation Boundary

This report compares Spark treatment variants against each other on the current machine. It does
not compare absolute Spark milliseconds to the published Hadoop milliseconds because hardware,
engine, data curation and undisclosed controls differ. The ICCIT paper does not report `k`,
partition count or query seeds; this execution records assumed values explicitly.

| Setting | Value |
|---|---|
| Profile | `{profile}` |
| k (declared assumption) | `{k}` |
| partitions (declared assumption) | `{partitions}` |
| Exact oracle during performance run | `{validated}` |
| Spark driver memory | `{driver_memory}` |
| Spark master | `{spark_master}` |
| Bound mode | `{metrics['aes-dscp']['spark'].get('boundMode')}` |

## Published ICCIT Reference Only

| Paper dataset | Baseline Hadoop act WC | AES+DSCP Hadoop act WC | Reduction |
|---|---:|---:|---:|
| {profile} reference | {int(paper_base):,} ms | {int(paper_full):,} ms | {paper_reduction}% |

## Observed Spark Same-Machine Treatments

| Treatment | Algorithm ms | Reduction vs Spark baseline | Candidate filtered | Emitted records | Shuffle bytes | Partial MBR refs | Exact agreement |
|---|---:|---:|---:|---:|---:|---:|---|
{chr(10).join(rows)}

For paper-sized performance runs with exact validation disabled, exactness evidence must be
paired with the validated deterministic MBR suite and road smoke suite before results are used in
a manuscript. `Candidate filtered` includes the Rai-Lian indexed baseline's bound filtering;
the DSCP contribution is the additional filtering and runtime difference relative to that indexed
baseline, not the full value in that column. This runtime implements an STR-packed aggregate
R-tree per partition, selected exported
index levels and reducer-stage traversal of partial MBR references. The selected-level estimate
uses a deterministic representative candidate-instance probe sample and estimated reducer
subtree traversal work as a stand-in for the historical/uniform query calibration described in
the target paper; this is a declared Spark adaptation of Rai and Lian's index distribution
policy.
"""
Path(output).write_text(content)
print(f"comparisonReport={output}")
PY
