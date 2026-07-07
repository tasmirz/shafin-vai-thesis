#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SUITE_ID="${SUITE_ID:-full-comparison-$(date -u +%Y%m%dT%H%M%SZ)}"
PROFILE="${PROFILE:-road-smoke}"
BUILD_IMAGE="${BUILD_IMAGE:-1}"
K="${K:-10}"
PARTITIONS="${PARTITIONS:-8}"
VALIDATE_EXACT="${VALIDATE_EXACT:-true}"
SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-2g}"
SPARK_MASTER="${SPARK_MASTER:-local[4]}"
TRACE_LIMIT="${TRACE_LIMIT:-25}"
REUSE_EXISTING_RUNS="${REUSE_EXISTING_RUNS:-false}"
RUN_HADOOP="${RUN_HADOOP:-true}"
RUN_SPARK="${RUN_SPARK:-true}"

case "$PROFILE" in
  smartphone)
    CSV_PATH="${CSV_PATH:-$ROOT_DIR/datasets-curated/smartphone-paper.csv}"
    DATASET_MANIFEST="${DATASET_MANIFEST:-$ROOT_DIR/reports/datasets/smartphone-paper.json}"
    # Paper: ICCIT 2025, Table II — synthetic smartphone (750 objects, 20 queries per Table I)
    PAPER_BASELINE_MS=66520
    PAPER_AES_ONLY_MS=44760
    PAPER_DSCP_ONLY_MS=58484
    PAPER_AES_DSCP_MS=43757
    PAPER_BASELINE_CC="1.4179×10^11"
    PAPER_REDUCTION=34.2
    PAPER_REDUCTION_AES=32.7
    PAPER_REDUCTION_DSCP=12.0
    PAPER_REDUCTION_FULL=34.2
    PAPER_DATASET="Synthetic smartphone"
    ;;
  road-smoke)
    CSV_PATH="${CSV_PATH:-$ROOT_DIR/datasets-curated/bangladesh-road-smoke.csv}"
    DATASET_MANIFEST="${DATASET_MANIFEST:-$ROOT_DIR/reports/datasets/bangladesh-road-smoke.json}"
    PAPER_BASELINE_MS=56274
    PAPER_AES_ONLY_MS=42967
    PAPER_DSCP_ONLY_MS=46848
    PAPER_AES_DSCP_MS=42366
    PAPER_BASELINE_CC="1.54×10^10"
    PAPER_REDUCTION=24.7
    PAPER_REDUCTION_AES=23.6
    PAPER_REDUCTION_DSCP=16.8
    PAPER_REDUCTION_FULL=24.7
    PAPER_DATASET="Bangladesh road / OSM (smoke)"
    ;;
  road-full)
    CSV_PATH="${CSV_PATH:-$ROOT_DIR/datasets-curated/bangladesh-road-paper.csv}"
    DATASET_MANIFEST="${DATASET_MANIFEST:-$ROOT_DIR/reports/datasets/bangladesh-road-paper.json}"
    PAPER_BASELINE_MS=56274
    PAPER_AES_ONLY_MS=42967
    PAPER_DSCP_ONLY_MS=46848
    PAPER_AES_DSCP_MS=42366
    PAPER_BASELINE_CC="1.54×10^10"
    PAPER_REDUCTION=24.7
    PAPER_REDUCTION_AES=23.6
    PAPER_REDUCTION_DSCP=16.8
    PAPER_REDUCTION_FULL=24.7
    PAPER_DATASET="Bangladesh road / OSM"
    if [[ "${ALLOW_FULL_ROAD:-false}" != "true" ]]; then
      echo "road-full requires ALLOW_FULL_ROAD=true (98,451 objects)." >&2
      exit 2
    fi
    ;;
  road-full-20q)
    CSV_PATH="${CSV_PATH:-$ROOT_DIR/datasets-curated/bangladesh-road-paper.csv}"
    DATASET_MANIFEST="${DATASET_MANIFEST:-$ROOT_DIR/reports/datasets/bangladesh-road-paper.json}"
    QUERY_SET_PATH="${QUERY_SET_PATH:-$ROOT_DIR/datasets-curated/bangladesh-road-queries-20.csv}"
    QUERY_SET_MANIFEST="${QUERY_SET_MANIFEST:-$ROOT_DIR/reports/datasets/bangladesh-road-queries-20.json}"
    PAPER_BASELINE_MS=56274
    PAPER_AES_ONLY_MS=42967
    PAPER_DSCP_ONLY_MS=46848
    PAPER_AES_DSCP_MS=42366
    PAPER_BASELINE_CC="1.54×10^10"
    PAPER_REDUCTION=24.7
    PAPER_REDUCTION_AES=23.6
    PAPER_REDUCTION_DSCP=16.8
    PAPER_REDUCTION_FULL=24.7
    PAPER_DATASET="Bangladesh road / OSM (20 queries)"
    if [[ "${ALLOW_FULL_ROAD:-false}" != "true" ]]; then
      echo "road-full-20q requires ALLOW_FULL_ROAD=true." >&2
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

VARIANTS=(baseline dscp-only aes-only aes-dscp)
SPARK_VARIANTS=(baseline dscp-only aes-only aes-dscp improved-baseline improved-dscp-only improved-aes-only improved-aes-dscp)
HADOOP_RUNS=()
SPARK_RUNS=()
ALL_RUNS=()

run_hadoop_variant() {
  local build="$1"
  local variant="$2"
  local run_id="${SUITE_ID}-hadoop-${variant}"
  HADOOP_RUNS+=("$run_id")
  ALL_RUNS+=("$run_id")
  if [[ "$REUSE_EXISTING_RUNS" == "true" && -f "$ROOT_DIR/reports/runs/$run_id/metrics.json" ]]; then
    echo "== Reusing existing Hadoop run: $run_id =="
    return
  fi
  echo "== Running Hadoop PTD: $variant =="
  BUILD_IMAGE="$build" RUN_ID="$run_id" ALGORITHM="$variant" CSV_PATH="$CSV_PATH" \
    K="$K" PARTITIONS="$PARTITIONS" VALIDATE_EXACT="$VALIDATE_EXACT" \
    scripts/research/run_hadoop_csv_benchmark.sh
}

run_spark_variant() {
  local build="$1"
  local variant="$2"
  local run_id="${SUITE_ID}-spark-${variant}"
  SPARK_RUNS+=("$run_id")
  ALL_RUNS+=("$run_id")
  if [[ "$REUSE_EXISTING_RUNS" == "true" && -f "$ROOT_DIR/reports/runs/$run_id/metrics.json" ]]; then
    echo "== Reusing existing Spark run: $run_id =="
    return
  fi
  echo "== Running Spark PTD: $variant =="
  local script="scripts/research/run_csv_benchmark.sh"
  if [[ "$variant" == improved-* ]]; then
    script="scripts/research/run_improved_csv_benchmark.sh"
  fi
  RUN_ID="$run_id" ALGORITHM="$variant" CSV_PATH="$CSV_PATH" \
    DATASET_MANIFEST="$DATASET_MANIFEST" \
    QUERY_SET_PATH="${QUERY_SET_PATH:-}" QUERY_SET_MANIFEST="${QUERY_SET_MANIFEST:-}" \
    K="$K" PARTITIONS="$PARTITIONS" VALIDATE_EXACT="$VALIDATE_EXACT" \
    RUN_LOCAL_ORACLE="$VALIDATE_EXACT" BUILD_IMAGE="$build" \
    SPARK_DRIVER_MEMORY="$SPARK_DRIVER_MEMORY" SPARK_MASTER="$SPARK_MASTER" \
    TRACE_LIMIT="$TRACE_LIMIT" \
    "$script"
}

echo "========================================================================"
echo " Full Comparison Suite: $SUITE_ID"
echo " Profile: $PROFILE | k=$K | partitions=$PARTITIONS | validateExact=$VALIDATE_EXACT"
echo " Spark: $SPARK_MASTER / $SPARK_DRIVER_MEMORY"
echo " Run Hadoop: $RUN_HADOOP | Run Spark: $RUN_SPARK"
echo "========================================================================"

export SUITE_ID

if [[ "$RUN_HADOOP" == "true" ]]; then
  echo ""
  echo "=== Phase 1: ICCIT Hadoop MapReduce variants ==="
  hbuild="$BUILD_IMAGE"
  for variant in "${VARIANTS[@]}"; do
    run_hadoop_variant "$hbuild" "$variant"
    hbuild=0
  done
fi

if [[ "$RUN_SPARK" == "true" ]]; then
  echo ""
  echo "=== Phase 2: Apache Spark variants ==="
  sbuild="$BUILD_IMAGE"
  for variant in "${SPARK_VARIANTS[@]}"; do
    run_spark_variant "$sbuild" "$variant"
    sbuild=0
  done
fi

echo ""
echo "=== Phase 3: Comparison report ==="
python3 scripts/research/compare_runs.py "${ALL_RUNS[@]}"

echo ""
echo "=== Phase 4: Paper validation ==="
python3 - "$SUITE_ID" "$PROFILE" "$PAPER_DATASET" "$K" "$PARTITIONS" \
  "$VALIDATE_EXACT" "$PAPER_BASELINE_MS" "$PAPER_AES_ONLY_MS" "$PAPER_DSCP_ONLY_MS" \
  "$PAPER_AES_DSCP_MS" "$PAPER_BASELINE_CC" "$PAPER_REDUCTION" \
  "$PAPER_REDUCTION_AES" "$PAPER_REDUCTION_DSCP" "$PAPER_REDUCTION_FULL" \
  "$SPARK_DRIVER_MEMORY" "$SPARK_MASTER" "$RUN_HADOOP" "$RUN_SPARK" \
  <<'VALIDATE_PY'
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path

suite, profile, dataset_name, k, partitions, validated, paper_base, paper_aes, paper_dscp, paper_full, paper_cc, paper_reduction, paper_red_aes, paper_red_dscp, paper_red_full, driver_memory, spark_master, run_hadoop, run_spark = sys.argv[1:]
k = int(k)
partitions = int(partitions)
paper_base = int(paper_base)
paper_aes = int(paper_aes)
paper_dscp = int(paper_dscp)
paper_full = int(paper_full)
paper_reduction = float(paper_reduction)
paper_red_aes = float(paper_red_aes)
paper_red_dscp = float(paper_red_dscp)
paper_red_full = float(paper_red_full)
run_hadoop = run_hadoop.lower() in ("1", "true")
run_spark = run_spark.lower() in ("1", "true")
validated_flag = validated.lower() in ("1", "true")

root = Path("reports/runs")
variants = ("baseline", "dscp-only", "aes-only", "aes-dscp", "improved-baseline", "improved-dscp-only", "improved-aes-only", "improved-aes-dscp")
engines = []
if run_hadoop:
  engines.append("hadoop")
if run_spark:
  engines.append("spark")

def load_metrics(run_id):
  path = root / run_id / "metrics.json"
  if not path.exists():
    return None
  return json.loads(path.read_text())

def safe(val, default="n/a"):
  return val if val is not None else default

rows = []
hadoop_data = {}
spark_data = {}
validation_ok = True
validation_messages = []

for engine in engines:
  for variant in variants:
    run_id = f"{suite}-{engine}-{variant}"
    metrics = load_metrics(run_id)
    if metrics is None:
      rows.append(f"| {engine} | {variant} | **missing** | - | - | - | - | - |")
      continue
    spark = metrics["spark"]
    val = metrics["validation"]
    elapsed = spark.get("algorithmElapsedMs")
    prune = spark.get("avgPruneRatio")
    prune_text = f"{prune*100:.2f}%" if prune is not None else "n/a"
    emitted_val = spark.get("totalEmittedRecords")
    emitted_text = f"{emitted_val:,}" if emitted_val is not None else "n/a"
    aer = safe(spark.get("avgAER"))
    exact = val.get("exactTopKAgreement")
    exact_text = "not run" if exact is None else str(exact)
    false_prunes = safe(spark.get("falsePruneCount"))
    shuffle_val = spark.get("totalShuffleBytes")
    shuffle_text = f"{shuffle_val:,}" if shuffle_val is not None else "n/a"
    elapsed_text = f"{elapsed:,} ms" if elapsed is not None else "n/a"
    rows.append(
        f"| {engine} | {variant} | {elapsed_text} | {prune_text} | {emitted_text} | "
        f"{aer} | {shuffle_text} | {exact_text} |")
    if engine == "hadoop":
      hadoop_data[variant] = metrics
    else:
      spark_data[variant] = metrics

    if validated_flag and elapsed is not None:
      if exact is False:
        validation_messages.append(f"FAIL: {run_id} exactAgreement=false")
        validation_ok = False
      if false_prunes is not None and false_prunes > 0:
        validation_messages.append(f"FAIL: {run_id} falsePrunes={false_prunes}")
        validation_ok = False

engine_header = ""
for e in engines:
  engine_header += f" {e} |"
engine_header = engine_header.rstrip(" |")

has_both = "hadoop" in hadoop_data and "spark" in spark_data
h_base = hadoop_data.get("baseline", {}).get("spark", {}).get("algorithmElapsedMs") if "hadoop" in hadoop_data else None
h_full = hadoop_data.get("aes-dscp", {}).get("spark", {}).get("algorithmElapsedMs") if "hadoop" in hadoop_data else None
s_base = spark_data.get("baseline", {}).get("spark", {}).get("algorithmElapsedMs") if "spark" in spark_data else None
s_full = spark_data.get("aes-dscp", {}).get("spark", {}).get("algorithmElapsedMs") if "spark" in spark_data else None
h_reduction = ((h_base - h_full) / h_base * 100) if h_base and h_full else 0
s_reduction = ((s_base - s_full) / s_base * 100) if s_base and s_full else 0

report = f"""# Full Comparison Suite: `{suite}`

Generated: {datetime.now(timezone.utc).isoformat()}

## Configuration

| Setting | Value |
|---|---|
| Profile | `{profile}` / {dataset_name} |
| k | {k} |
| partitions | {partitions} |
| Exact validation during run | {validated} |
| Spark driver memory | {driver_memory} |
| Spark master | {spark_master} |

## Published ICCIT Paper (Hadoop MapReduce) Reference

These reference values are from the ICCIT 2025 paper. Hardware and runtime conditions differ from the observed runs below. Assumptions for `k`, partitions, and query seeds are declared protocol values. The paper uses 20 random queries averaged (Table II, III).

**Table II — Improvement of Proposed Algorithm (act WC):**

| Dataset | Baseline (ms) | AES-only (ms) | DSCP-only (ms) | AES+DSCP (ms) | act CC | Reduction |
|---|---|---|---|---|---|---|
| {dataset_name} | {paper_base:,} | {paper_aes:,} | {paper_dscp:,} | {paper_full:,} | {paper_cc} | {paper_reduction}% |

**Table III — Ablation: Wall Clock Time Reduction vs Baseline (%):**

| Dataset | AES-only | DSCP-only | AES+DSCP |
|---|---|---|---|
| {dataset_name} | {paper_red_aes}% | {paper_red_dscp}% | {paper_red_full}% |

## Observed Results

### Per-Variant Comparison

| Engine | Variant | Algorithm ms | Prune ratio | Emitted records | AER | Shuffle bytes | Exact agreement |
|---|---|---|---|---|---|---|---|
{chr(10).join(rows)}

### Within-Engine Reduction vs Baseline

| Engine | Baseline (ms) | AES-only (ms) | DSCP-only (ms) | AES+DSCP (ms) | Reduction (AES+DSCP vs Baseline) |
|---|---|---|---|---|---|---|
"""
if "hadoop" in hadoop_data and "baseline" in hadoop_data:
  h_aes = hadoop_data["aes-only"]["spark"]["algorithmElapsedMs"]
  h_dscp = hadoop_data["dscp-only"]["spark"]["algorithmElapsedMs"]
  report += f"| hadoop (MapReduce) | {h_base:,} | {h_aes:,} | {h_dscp:,} | {h_full:,} | {h_reduction:.2f}% |\n"
if "spark" in spark_data and "baseline" in spark_data:
  s_aes = spark_data["aes-only"]["spark"]["algorithmElapsedMs"]
  s_dscp = spark_data["dscp-only"]["spark"]["algorithmElapsedMs"]
  report += f"| spark | {s_base:,} | {s_aes:,} | {s_dscp:,} | {s_full:,} | {s_reduction:.2f}% |\n"
if "spark" in spark_data and "improved-baseline" in spark_data:
  s_imp_base = spark_data["improved-baseline"]["spark"]["algorithmElapsedMs"]
  s_imp_aes = spark_data["improved-aes-only"]["spark"]["algorithmElapsedMs"]
  s_imp_dscp = spark_data["improved-dscp-only"]["spark"]["algorithmElapsedMs"]
  s_imp_full = spark_data["improved-aes-dscp"]["spark"]["algorithmElapsedMs"]
  s_imp_reduction = ((s_imp_base - s_imp_full) / s_imp_base * 100) if s_imp_base else 0
  report += f"| spark (HGTP) | {s_imp_base:,} | {s_imp_aes:,} | {s_imp_dscp:,} | {s_imp_full:,} | {s_imp_reduction:.2f}% |\n"

report += f"""
## Interpretation

- **Rai-Lian baseline** = `baseline` variant: aR-tree score-bound baseline without AES or DSCP extensions
- **ICCTT Hadoop** = all 4 variants run on Hadoop MapReduce engine
- **Spark** = the same 4 treatment variants run on Apache Spark RDD engine
- The Rai-Lian baseline and ICCIT variants share the same `PtdAlgorithm` definitions: the only difference is the execution engine (Hadoop vs Spark)
- Published Hadoop ms are reference only — no cross-engine speedup claim is valid given differing hardware
"""

if validation_messages:
  report += f"""
## Validation Issues

{chr(10).join(f'- {msg}' for msg in validation_messages)}
"""
  report += "\n**Validation result: FAILED**\n"
else:
  report += "\n**Validation result: All checks passed**\n"

output = Path(f"reports/validation/{suite}-full-comparison.md")
output.parent.mkdir(parents=True, exist_ok=True)
output.write_text(report)
print(f"fullComparisonReport={output}")
VALIDATE_PY

echo "Generating comparison plots..."
.venv/bin/python3 scripts/research/generate_plots.py "$SUITE_ID" || echo "Plot generation skipped or failed."

echo ""
echo "=== Summary ==="
echo "Suite ID: $SUITE_ID"
echo "Report: reports/validation/${SUITE_ID}-full-comparison.md"
echo "All run artifacts: reports/runs/${SUITE_ID}-*"
echo "To compare specific runs: python3 scripts/research/compare_runs.py <run_ids...>"
echo "Done."
