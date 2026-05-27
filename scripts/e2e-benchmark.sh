#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/docker-compose.e2e.yml"
REPORT_DIR="${E2E_REPORT_DIR:-$ROOT_DIR/reports/e2e}"
if [[ "$REPORT_DIR" != /* ]]; then
  REPORT_DIR="$ROOT_DIR/$REPORT_DIR"
fi
TOPIC="${TOPIC:-thesis.raw.incomplete}"
MQTT_TOPIC="${MQTT_TOPIC:-thesis/raw}"
OBJECTS="${OBJECTS:-500}"
DIMENSIONS="${DIMENSIONS:-4}"
QUERIES="${QUERIES:-2}"
K="${K:-10}"
MISSING_RATE="${MISSING_RATE:-0.35}"
RATE_PER_SECOND="${RATE_PER_SECOND:-200}"
QOS="${QOS:-1}"
PARTITIONS="${PARTITIONS:-${PARALLELISM:-4}}"
SYNOPSIS_BINS="${SYNOPSIS_BINS:-8}"
ALGORITHM="${ALGORITHM:-aes-dscp}"
VALIDATE_EXACT="${VALIDATE_EXACT:-true}"
RUN_LOCAL_ORACLE="${RUN_LOCAL_ORACLE:-$VALIDATE_EXACT}"
SEED="${SEED:-7}"
DATASET="${DATASET:-synthetic}"
DATASET_PATH="${DATASET_PATH:-}"
DATASET_MANIFEST="${DATASET_MANIFEST:-}"
MAX_EVENTS="${MAX_EVENTS:-0}"
RUN_ID="${RUN_ID:-stream-$(date -u +%Y%m%dT%H%M%SZ)}"
if ((MAX_EVENTS <= 0)); then
  MAX_EVENTS=$((OBJECTS * QUERIES))
fi
INGRESS_READY_SLEEP_SECONDS="${INGRESS_READY_SLEEP_SECONDS:-10}"
KAFKA_WAIT_SECONDS="${KAFKA_WAIT_SECONDS:-300}"
if [[ "$DATASET" == "all" ]]; then
  TOPIC_MAPPINGS="${TOPIC_MAPPINGS:-thesis/raw/intel=thesis.raw.intel,thesis/raw/pump=thesis.raw.pump,thesis/raw/gas=thesis.raw.gas}"
  EXPECTED_MESSAGES="${EXPECTED_MESSAGES:-$((MAX_EVENTS * 3))}"
else
  TOPIC_MAPPINGS="${TOPIC_MAPPINGS:-$MQTT_TOPIC=$TOPIC}"
  EXPECTED_MESSAGES="${EXPECTED_MESSAGES:-$MAX_EVENTS}"
fi

mkdir -p "$REPORT_DIR"
REPORT_DISPLAY="${REPORT_DIR#"$ROOT_DIR/"}"
if [[ "$REPORT_DISPLAY" == "$REPORT_DIR" ]]; then
  REPORT_DISPLAY="$REPORT_DIR"
fi
if [[ ! "$RUN_ID" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]]; then
  echo "Invalid RUN_ID: use letters, digits, '.', '_' and '-' only" >&2
  exit 1
fi
if [[ -e "$ROOT_DIR/reports/runs/$RUN_ID" ]]; then
  echo "Run already exists: reports/runs/$RUN_ID" >&2
  exit 1
fi

now_ms() {
  date +%s%3N
}

topic_list() {
  IFS=',' read -ra mappings <<<"$TOPIC_MAPPINGS"
  for mapping in "${mappings[@]}"; do
    echo "${mapping#*=}"
  done
}

kafka_count() {
  if [[ "$DATASET" == "all" ]]; then
    timeout 20s docker compose -f "$COMPOSE_FILE" exec -T kafka \
      /opt/kafka/bin/kafka-get-offsets.sh \
        --bootstrap-server kafka:9092 \
        --topic 'thesis.raw.*' 2>/dev/null \
      | awk -F: '{sum += $3} END {print sum + 0}'
    return
  fi

  local total=0
  local count=0
  while read -r topic; do
    [[ -z "$topic" ]] && continue
    count="$(
      timeout 20s docker compose -f "$COMPOSE_FILE" exec -T kafka \
        /opt/kafka/bin/kafka-get-offsets.sh \
          --bootstrap-server kafka:9092 \
          --topic "$topic" 2>/dev/null \
        | awk -F: '{sum += $3} END {print sum + 0}'
    )"
    count="${count:-0}"
    total=$((total + count))
  done < <(topic_list)
  echo "$total"
}

wait_for_messages() {
  local deadline=$((SECONDS + KAFKA_WAIT_SECONDS))
  local count=0
  while true; do
    count="$(kafka_count)"
    if ((count >= EXPECTED_MESSAGES)); then
      echo "$count"
      return 0
    fi
    if ((SECONDS >= deadline)); then
      echo "Timed out waiting for Kafka messages: got $count, expected $EXPECTED_MESSAGES" >&2
      return 1
    fi
    sleep 1
  done
}

cd "$ROOT_DIR"

spark_dataset_path="$DATASET_PATH"
spark_dataset_mount=()
if [[ "$DATASET" == "csv" ]]; then
  if [[ -z "$DATASET_PATH" || ! -f "$DATASET_PATH" ]]; then
    echo "DATASET=csv requires a readable DATASET_PATH" >&2
    exit 1
  fi
  spark_dataset_path="/opt/spark/input/events.csv"
  spark_dataset_mount=(-v "$DATASET_PATH:$spark_dataset_path:ro,Z")
fi

echo "Starting and configuring clean MQTT/Kafka/Spark stack..."
RESET=1 TOPIC_MAPPINGS="$TOPIC_MAPPINGS" "$ROOT_DIR/scripts/setup-services.sh"

echo "Waiting ${INGRESS_READY_SLEEP_SECONDS}s for the EMQX Kafka sink to warm up..."
sleep "$INGRESS_READY_SLEEP_SECONDS"

echo "Publishing $EXPECTED_MESSAGES incomplete records to MQTT..."
publish_start_ms="$(now_ms)"
scripts/setup-venv.sh >/dev/null
.venv/bin/python scripts/simulator.py \
  --mqtt-host=localhost \
  --mqtt-port=18884 \
  --topic="$MQTT_TOPIC" \
  --dataset="$DATASET" \
  --dataset-path="$DATASET_PATH" \
  --objects="$OBJECTS" \
  --dimensions="$DIMENSIONS" \
  --queries="$QUERIES" \
  --missing-rate="$MISSING_RATE" \
  --rate-per-second="$RATE_PER_SECOND" \
  --qos="$QOS" \
  --repeat=1 \
  --max-events="$MAX_EVENTS" \
  --seed="$SEED"
publish_end_ms="$(now_ms)"

echo "Waiting for EMQX Kafka sink to deliver all records..."
kafka_messages="$(wait_for_messages)"
kafka_ready_ms="$(now_ms)"

SPARK_SUBMIT_LOG="$REPORT_DIR/spark-submit.log"
SPARK_LOG="$REPORT_DIR/spark.log"
echo "Running bounded Spark Kafka drain..."
spark_start_ms="$(now_ms)"
>"$SPARK_SUBMIT_LOG"

docker compose -f "$COMPOSE_FILE" run --rm "${spark_dataset_mount[@]}" spark-submit \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --class com.thesis.topk.spark.ProbabilisticTopKSparkJob \
  /opt/spark/app/topk-spark.jar \
  --source=kafka \
  --kafkaBootstrap=kafka:9092 \
  --kafkaTopic="$TOPIC" \
  --kafkaGroupId="probabilistic-topk-spark-e2e-$(date +%s)" \
  --checkpointLocation="/tmp/ptd-checkpoints/$RUN_ID" \
  --expectedMessages="$EXPECTED_MESSAGES" \
  --dataset="$DATASET" \
  --datasetPath="$spark_dataset_path" \
  --objects="$OBJECTS" \
  --dimensions="$DIMENSIONS" \
  --queries="$QUERIES" \
  --k="$K" \
  --missingRate="$MISSING_RATE" \
  --partitions="$PARTITIONS" \
  --synopsisBins="$SYNOPSIS_BINS" \
  --algorithm="$ALGORITHM" \
  --maxEvents="$MAX_EVENTS" \
  --seed="$SEED" \
  --validateExact="$VALIDATE_EXACT" \
  --sparkMaster=spark://spark-master:7077 >>"$SPARK_SUBMIT_LOG" 2>&1
spark_end_ms="$(now_ms)"
cp "$SPARK_SUBMIT_LOG" "$SPARK_LOG"

topk_results="$(grep -c 'TopKResult{' "$SPARK_LOG" || true)"
if grep -q "exactAgreement=false" "$SPARK_LOG"; then
  cat "$SPARK_LOG" >&2
  exit 1
fi
if grep -Eq "falsePrunes=[1-9][0-9]*" "$SPARK_LOG"; then
  cat "$SPARK_LOG" >&2
  exit 1
fi
publish_ms=$((publish_end_ms - publish_start_ms))
ingress_ms=$((kafka_ready_ms - publish_start_ms))
spark_ms=$((spark_end_ms - spark_start_ms))
total_ms=$((spark_end_ms - publish_start_ms))
publish_rate="$(awk "BEGIN {printf \"%.2f\", $EXPECTED_MESSAGES / ($publish_ms / 1000)}")"
end_to_end_rate="$(awk "BEGIN {printf \"%.2f\", $EXPECTED_MESSAGES / ($total_ms / 1000)}")"

SUMMARY="$REPORT_DIR/summary.md"
CSV="$REPORT_DIR/summary.csv"

cat >"$SUMMARY" <<EOF_SUMMARY
# E2E Benchmark

Pipeline:

\`\`\`text
PythonSimulator -> EMQX MQTT -> EMQX Kafka sink -> Kafka -> Apache Spark Structured Streaming bounded reader -> SparkTopKEngine -> TopKResult
\`\`\`

Config:

- objects: $OBJECTS
- queries: $QUERIES
- dimensions: $DIMENSIONS
- k: $K
- missingRate: $MISSING_RATE
- publisherRatePerSecond: $RATE_PER_SECOND
- mqttQos: $QOS
- sparkPartitions: $PARTITIONS
- synopsisBins: $SYNOPSIS_BINS
- algorithm: $ALGORITHM
- expectedMessages: $EXPECTED_MESSAGES
- kafkaMessages: $kafka_messages
- topKResults: $topk_results
- dataset: $DATASET
- datasetPath: $DATASET_PATH
- topicMappings: $TOPIC_MAPPINGS
- savedRunId: $RUN_ID

Timing:

- mqttPublishMs: $publish_ms
- mqttToKafkaReadyMs: $ingress_ms
- sparkDrainMs: $spark_ms
- totalE2EMs: $total_ms
- publishRateMessagesPerSecond: $publish_rate
- endToEndRateMessagesPerSecond: $end_to_end_rate

Artifacts:

- Spark log: \`$REPORT_DISPLAY/spark.log\`
- Spark submit log: \`$REPORT_DISPLAY/spark-submit.log\`
- CSV: \`$REPORT_DISPLAY/summary.csv\`
EOF_SUMMARY

cat >"$CSV" <<EOF_CSV
objects,queries,dimensions,k,missing_rate,mqtt_qos,spark_partitions,synopsis_bins,algorithm,expected_messages,kafka_messages,topk_results,publish_ms,ingress_ms,spark_ms,total_ms,publish_rate_msg_s,e2e_rate_msg_s,dataset,topic_mappings
$OBJECTS,$QUERIES,$DIMENSIONS,$K,$MISSING_RATE,$QOS,$PARTITIONS,$SYNOPSIS_BINS,$ALGORITHM,$EXPECTED_MESSAGES,$kafka_messages,$topk_results,$publish_ms,$ingress_ms,$spark_ms,$total_ms,$publish_rate,$end_to_end_rate,$DATASET,"$TOPIC_MAPPINGS"
EOF_CSV

cat "$SUMMARY"

ALGORITHM_LOG="$REPORT_DIR/algorithm-validation.log"
algorithm_artifact=()
if [[ "$RUN_LOCAL_ORACLE" == "1" || "$RUN_LOCAL_ORACLE" == "true" ]]; then
  echo "Running untimed exactness validation for the saved stream snapshot configuration..."
  mvn -q -DskipTests compile exec:java \
    -Dexec.mainClass=com.thesis.topk.benchmark.TopKBenchmark \
    -Dexec.args="--dataset=$DATASET --datasetPath=$DATASET_PATH --objects=$OBJECTS --dimensions=$DIMENSIONS --queries=$QUERIES --k=$K --missingRate=$MISSING_RATE --seed=$SEED --partitions=$PARTITIONS --synopsisBins=$SYNOPSIS_BINS --maxEvents=$MAX_EVENTS" \
    >"$ALGORITHM_LOG" 2>&1
  if grep -q "topKAgreement=false" "$ALGORITHM_LOG"; then
    cat "$ALGORITHM_LOG" >&2
    exit 1
  fi
  algorithm_artifact=(--algorithm-log "$ALGORITHM_LOG")
fi

dataset_artifact=()
dataset_manifest_artifact=()
if [[ -n "$DATASET_PATH" && -f "$DATASET_PATH" ]]; then
  dataset_artifact=(--dataset-file "$DATASET_PATH")
fi
if [[ -n "$DATASET_MANIFEST" && -f "$DATASET_MANIFEST" ]]; then
  dataset_manifest_artifact=(--dataset-manifest "$DATASET_MANIFEST")
fi
python3 scripts/research/archive_run.py \
  --run-id "$RUN_ID" \
  --mode stream \
  --spark-log "$SPARK_LOG" \
  "${algorithm_artifact[@]}" \
  --e2e-summary "$CSV" \
  "${dataset_artifact[@]}" \
  "${dataset_manifest_artifact[@]}" \
  --parameter "dataset=$DATASET" \
  --parameter "algorithm=$ALGORITHM" \
  --parameter "validateExact=$VALIDATE_EXACT" \
  --parameter "objects=$OBJECTS" \
  --parameter "queries=$QUERIES" \
  --parameter "dimensions=$DIMENSIONS" \
  --parameter "k=$K" \
  --parameter "partitions=$PARTITIONS" \
  --parameter "seed=$SEED" \
  --parameter "missingRate=$MISSING_RATE" \
  --parameter "mqttQos=$QOS"
