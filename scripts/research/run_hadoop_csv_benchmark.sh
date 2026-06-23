#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/docker-compose.hadoop.yml"
CSV_PATH="${CSV_PATH:-$ROOT_DIR/tests/fixtures/paper/mbr-pruning.csv}"
RUN_ID="${RUN_ID:-hadoop-csv-$(date -u +%Y%m%dT%H%M%SZ)}"
ALGORITHM="${ALGORITHM:-aes-dscp}"
K="${K:-2}"
PARTITIONS="${PARTITIONS:-2}"
VALIDATE_EXACT="${VALIDATE_EXACT:-true}"
BUILD_IMAGE="${BUILD_IMAGE:-1}"
HADOOP_BASE="/tmp/ptd-benchlab/$RUN_ID"
RUN_TMP="$(mktemp -d)"

cleanup() {
  status=$?
  if ((status == 0)); then
    rm -rf "$RUN_TMP"
  else
    echo "Hadoop CSV benchmark failed; diagnostic logs retained at $RUN_TMP" >&2
  fi
}
trap cleanup EXIT

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

mvn -q -DskipTests package
if [[ "$BUILD_IMAGE" == "1" || "$BUILD_IMAGE" == "true" ]]; then
  docker compose -f "$COMPOSE_FILE" build
fi
docker compose -f "$COMPOSE_FILE" up -d namenode datanode resourcemanager nodemanager historyserver

for _ in {1..60}; do
  if docker compose -f "$COMPOSE_FILE" run --rm hadoop-client \
      client bash -lc 'hdfs dfs -ls / >/dev/null && yarn node -list >/dev/null' \
      >"$RUN_TMP/cluster-ready.log" 2>&1; then
    break
  fi
  sleep 2
done

docker compose -f "$COMPOSE_FILE" run --rm hadoop-client client bash -lc "
  set -euo pipefail
  hdfs dfsadmin -safemode wait
  hdfs dfs -rm -r -f '$HADOOP_BASE' >/dev/null 2>&1 || true
  hdfs dfs -mkdir -p '$HADOOP_BASE/input'
  hdfs dfs -put -f '/workspace/${CSV_PATH#$ROOT_DIR/}' '$HADOOP_BASE/input/events.csv'
  hadoop jar /workspace/target/probabilistic-topk-spark-1.0.0-SNAPSHOT-shaded.jar \
    com.thesis.topk.hadoop.ProbabilisticTopKHadoopJob \
    --input='$HADOOP_BASE/input/events.csv' \
    --output='$HADOOP_BASE/output' \
    --algorithm='$ALGORITHM' \
    --k='$K' \
    --partitions='$PARTITIONS' \
    --validateExact='$VALIDATE_EXACT' \
    --local=false
" >"$RUN_TMP/hadoop.log" 2>&1

grep -q "engine=apache-hadoop" "$RUN_TMP/hadoop.log"
grep -q "query=" "$RUN_TMP/hadoop.log"
if [[ "$VALIDATE_EXACT" == "1" || "$VALIDATE_EXACT" == "true" ]] \
    && grep -q "exactAgreement=false" "$RUN_TMP/hadoop.log"; then
  cat "$RUN_TMP/hadoop.log" >&2
  exit 1
fi
if [[ "$VALIDATE_EXACT" == "1" || "$VALIDATE_EXACT" == "true" ]] \
    && grep -Eq "falsePrunes=[1-9][0-9]*" "$RUN_TMP/hadoop.log"; then
  cat "$RUN_TMP/hadoop.log" >&2
  exit 1
fi

python3 scripts/research/archive_run.py \
  --run-id "$RUN_ID" \
  --mode hadoop \
  --spark-log "$RUN_TMP/hadoop.log" \
  --dataset-file "$CSV_PATH" \
  --parameter "dataset=csv" \
  --parameter "algorithm=$ALGORITHM" \
  --parameter "k=$K" \
  --parameter "partitions=$PARTITIONS" \
  --parameter "validateExact=$VALIDATE_EXACT" \
  --parameter "executionEngine=hadoop-mapreduce"

grep -E "^(engine=|rawEvents=|query=)" "$RUN_TMP/hadoop.log"
