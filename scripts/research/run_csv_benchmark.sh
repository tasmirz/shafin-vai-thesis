#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CSV_PATH="${CSV_PATH:-$ROOT_DIR/tests/fixtures/csv/smartphone-small.csv}"
DATASET_MANIFEST="${DATASET_MANIFEST:-}"
RUN_ID="${RUN_ID:-csv-$(date -u +%Y%m%dT%H%M%SZ)}"
K="${K:-2}"
PARTITIONS="${PARTITIONS:-2}"
SEED="${SEED:-42}"
SYNOPSIS_BINS="${SYNOPSIS_BINS:-4}"
ALGORITHM="${ALGORITHM:-aes-dscp}"
BUILD_IMAGE="${BUILD_IMAGE:-1}"
VALIDATE_EXACT="${VALIDATE_EXACT:-true}"
RUN_LOCAL_ORACLE="${RUN_LOCAL_ORACLE:-$VALIDATE_EXACT}"
SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-2g}"
SPARK_MASTER="${SPARK_MASTER:-local[4]}"
TRACE_LIMIT="${TRACE_LIMIT:-25}"
cleanup() {
  status=$?
  if ((status == 0)); then
    rm -rf "$RUN_TMP"
  else
    echo "CSV benchmark failed; diagnostic logs retained at $RUN_TMP" >&2
  fi
}

cd "$ROOT_DIR"
if [[ ! "$RUN_ID" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]]; then
  echo "Invalid RUN_ID: use letters, digits, '.', '_' and '-' only" >&2
  exit 1
fi
if [[ -e "$ROOT_DIR/reports/runs/$RUN_ID" ]]; then
  echo "Run already exists: reports/runs/$RUN_ID" >&2
  exit 1
fi
test -f "$CSV_PATH"
RUN_TMP="$(mktemp -d)"
trap cleanup EXIT
if [[ "$BUILD_IMAGE" == "1" || "$BUILD_IMAGE" == "true" ]]; then
  mvn -q -DskipTests package
  docker build -t thesis-topk-spark:local "$ROOT_DIR" >/dev/null
fi
docker run --rm \
  -v "$CSV_PATH:/opt/spark/input/events.csv:ro,Z" \
  thesis-topk-spark:local \
  /opt/spark/bin/spark-submit \
  --master "$SPARK_MASTER" \
  --driver-memory "$SPARK_DRIVER_MEMORY" \
  --class com.thesis.topk.spark.ProbabilisticTopKSparkJob \
  /opt/spark/app/topk-spark.jar \
  --source=simulator --dataset=csv --datasetPath=/opt/spark/input/events.csv \
  --k="$K" --partitions="$PARTITIONS" --seed="$SEED" --synopsisBins="$SYNOPSIS_BINS" \
  --algorithm="$ALGORITHM" \
  --traceLimit="$TRACE_LIMIT" \
  --validateExact="$VALIDATE_EXACT" --sparkMaster="$SPARK_MASTER" >"$RUN_TMP/spark.log" 2>&1
if [[ "$RUN_LOCAL_ORACLE" == "1" || "$RUN_LOCAL_ORACLE" == "true" ]]; then
  mvn -q -DskipTests compile exec:java \
    -Dexec.mainClass=com.thesis.topk.benchmark.TopKBenchmark \
    -Dexec.args="--dataset=csv --datasetPath=$CSV_PATH --k=$K --partitions=$PARTITIONS --seed=$SEED --synopsisBins=$SYNOPSIS_BINS" \
    >"$RUN_TMP/algorithm.log" 2>&1
fi

grep -q "engine=apache-spark source=simulator dataset=csv" "$RUN_TMP/spark.log"
grep -q "TopKResult{" "$RUN_TMP/spark.log"
if [[ "$VALIDATE_EXACT" == "1" || "$VALIDATE_EXACT" == "true" ]] \
    && grep -q "exactAgreement=false" "$RUN_TMP/spark.log"; then
  cat "$RUN_TMP/spark.log" >&2
  exit 1
fi
if [[ "$VALIDATE_EXACT" == "1" || "$VALIDATE_EXACT" == "true" ]] \
    && grep -Eq "falsePrunes=[1-9][0-9]*" "$RUN_TMP/spark.log"; then
  cat "$RUN_TMP/spark.log" >&2
  exit 1
fi
if [[ -f "$RUN_TMP/algorithm.log" ]] && grep -q "topKAgreement=false" "$RUN_TMP/algorithm.log"; then
  cat "$RUN_TMP/algorithm.log" >&2
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
if [[ -f "$RUN_TMP/algorithm.log" ]]; then
  archive_args+=(--algorithm-log "$RUN_TMP/algorithm.log")
fi
if [[ -n "$DATASET_MANIFEST" ]]; then
  archive_args+=(--dataset-manifest "$DATASET_MANIFEST")
fi
python3 scripts/research/archive_run.py "${archive_args[@]}"
grep -E "^(engine=|rawEvents=|TopKResult\\{|query=)" "$RUN_TMP/spark.log"
