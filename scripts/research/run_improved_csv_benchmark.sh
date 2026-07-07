#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CSV_PATH="${CSV_PATH:-$ROOT_DIR/tests/fixtures/paper/mbr-pruning.csv}"
K="${K:-5}"
PARTITIONS="${PARTITIONS:-2}"
ALGORITHM="${ALGORITHM:-improved-aes-dscp}"
SEED="${SEED:-7}"
SYNOPSIS_BINS="${SYNOPSIS_BINS:-4}"
VALIDATE_EXACT="${VALIDATE_EXACT:-true}"
SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-2g}"
SPARK_MASTER="${SPARK_MASTER:-local[*]}"
TRACE_LIMIT="${TRACE_LIMIT:-10}"
RUN_ID="${RUN_ID:-improved-csv-$(date -u +%Y%m%dT%H%M%SZ)}"
BUILD_IMAGE="${BUILD_IMAGE:-1}"

DATASET_MANIFEST=""
QUERY_SET_MANIFEST=""
QUERY_SET_PATH=""

RUN_TMP="$(mktemp -d)"
cleanup() {
  status=$?
  if ((status == 0)); then
    rm -rf "$RUN_TMP"
  else
    echo "Improved CSV benchmark failed; logs at $RUN_TMP" >&2
  fi
}
trap cleanup EXIT

cd "$ROOT_DIR"
if [[ ! "$RUN_ID" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]]; then
  echo "Invalid RUN_ID" >&2; exit 1
fi
if [[ -e "$ROOT_DIR/reports/runs/$RUN_ID" ]]; then
  echo "Run already exists: reports/runs/$RUN_ID" >&2; exit 1
fi
test -f "$CSV_PATH"

mvn -q -DskipTests package
if [[ "$BUILD_IMAGE" == "1" || "$BUILD_IMAGE" == "true" ]]; then
  docker build -t thesis-topk-spark:local .
fi

docker run --rm \
  -v "$CSV_PATH:/opt/spark/input/events.csv:ro,Z" \
  thesis-topk-spark:local \
  /opt/spark/bin/spark-submit \
  --driver-memory "$SPARK_DRIVER_MEMORY" \
  --master "$SPARK_MASTER" \
  --class com.thesis.topk.spark.ImprovedProbabilisticTopKSparkJob \
  /opt/spark/app/topk-spark.jar \
  --source=simulator \
  --dataset=csv \
  --datasetPath=/opt/spark/input/events.csv \
  --objects=200 \
  --k="$K" \
  --partitions="$PARTITIONS" \
  --algorithm="$ALGORITHM" \
  --seed="$SEED" \
  --synopsisBins="$SYNOPSIS_BINS" \
  --validateExact="$VALIDATE_EXACT" \
  --traceLimit="$TRACE_LIMIT" \
  >"$RUN_TMP/spark.log" 2>&1

grep -q "engine=improved-apache-spark" "$RUN_TMP/spark.log"
grep -q "TopKResult{" "$RUN_TMP/spark.log"
if [[ "$VALIDATE_EXACT" == "1" || "$VALIDATE_EXACT" == "true" ]] \
    && grep -q "exactAgreement=false" "$RUN_TMP/spark.log"; then
  cat "$RUN_TMP/spark.log" >&2
  exit 1
fi

archive_args=(
  --run-id "$RUN_ID"
  --mode csv
  --spark-log "$RUN_TMP/spark.log"
  --dataset-file "$CSV_PATH"
  --parameter "dataset=csv"
  --parameter "algorithm=$ALGORITHM"
  --parameter "k=$K"
  --parameter "partitions=$PARTITIONS"
  --parameter "seed=$SEED"
  --parameter "synopsisBins=$SYNOPSIS_BINS"
  --parameter "validateExact=$VALIDATE_EXACT"
  --parameter "sparkDriverMemory=$SPARK_DRIVER_MEMORY"
  --parameter "sparkMaster=$SPARK_MASTER"
  --parameter "traceLimit=$TRACE_LIMIT"
)
python3 scripts/research/archive_run.py "${archive_args[@]}"
grep -E "^(engine=|rawEvents=|TopKResult\{|query=)" "$RUN_TMP/spark.log"
