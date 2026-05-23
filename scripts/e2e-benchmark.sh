#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/docker-compose.e2e.yml"
REPORT_DIR="$ROOT_DIR/reports/e2e"
TOPIC="${TOPIC:-thesis.raw.incomplete}"
MQTT_TOPIC="${MQTT_TOPIC:-thesis/raw}"
OBJECTS="${OBJECTS:-500}"
DIMENSIONS="${DIMENSIONS:-4}"
QUERIES="${QUERIES:-2}"
K="${K:-10}"
MISSING_RATE="${MISSING_RATE:-0.35}"
RATE_PER_SECOND="${RATE_PER_SECOND:-200}"
QOS="${QOS:-0}"
WINDOW_MS="${WINDOW_MS:-10000}"
PARALLELISM="${PARALLELISM:-1}"
SYNOPSIS_BINS="${SYNOPSIS_BINS:-8}"
SEED="${SEED:-7}"
DATASET="${DATASET:-synthetic}"
DATASET_PATH="${DATASET_PATH:-}"
MAX_EVENTS="${MAX_EVENTS:-0}"
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

echo "Building shaded application jar..."
mvn -q -DskipTests package

echo "Starting and configuring clean MQTT/Kafka ingress stack..."
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

FLINK_SUBMIT_LOG="$REPORT_DIR/flink-submit.log"
FLINK_LOG="$REPORT_DIR/flink.log"
echo "Running bounded Flink Kafka drain..."
flink_start_ms="$(now_ms)"
>"$FLINK_SUBMIT_LOG"
if [[ "$DATASET" == "all" ]]; then
  docker compose -f "$COMPOSE_FILE" exec -T flink-jobmanager \
    /opt/flink/bin/flink run \
    -m flink-jobmanager:8081 \
    -c com.thesis.topk.flink.ProbabilisticTopKJob \
    /opt/flink/usrlib/topk.jar \
    --source=kafka \
    --kafkaBounded=true \
    --kafkaBootstrap=kafka:9092 \
    --kafkaGroupId="probabilistic-topk-e2e-$(date +%s)" \
    --dataset=all \
    --objects="$OBJECTS" \
    --dimensions="$DIMENSIONS" \
    --queries="$QUERIES" \
    --k="$K" \
    --missingRate="$MISSING_RATE" \
    --windowMs="$WINDOW_MS" \
    --parallelism="$PARALLELISM" \
    --synopsisBins="$SYNOPSIS_BINS" \
    --maxEvents="$MAX_EVENTS" \
    --seed="$SEED" >>"$FLINK_SUBMIT_LOG" 2>&1
else
  docker compose -f "$COMPOSE_FILE" exec -T flink-jobmanager \
    /opt/flink/bin/flink run \
    -m flink-jobmanager:8081 \
    -c com.thesis.topk.flink.ProbabilisticTopKJob \
    /opt/flink/usrlib/topk.jar \
    --source=kafka \
    --kafkaBounded=true \
    --kafkaBootstrap=kafka:9092 \
    --kafkaTopic="$TOPIC" \
    --kafkaGroupId="probabilistic-topk-e2e-$(date +%s)" \
    --dataset="$DATASET" \
    --datasetPath="$DATASET_PATH" \
    --objects="$OBJECTS" \
    --dimensions="$DIMENSIONS" \
    --queries="$QUERIES" \
    --k="$K" \
    --missingRate="$MISSING_RATE" \
    --windowMs="$WINDOW_MS" \
    --parallelism="$PARALLELISM" \
    --synopsisBins="$SYNOPSIS_BINS" \
    --maxEvents="$MAX_EVENTS" \
    --seed="$SEED" >>"$FLINK_SUBMIT_LOG" 2>&1
fi
flink_end_ms="$(now_ms)"
docker compose -f "$COMPOSE_FILE" logs --no-color flink-jobmanager flink-taskmanager >"$FLINK_LOG" 2>&1

topk_results="$(grep -c 'TopKResult{' "$FLINK_LOG" || true)"
publish_ms=$((publish_end_ms - publish_start_ms))
ingress_ms=$((kafka_ready_ms - publish_start_ms))
flink_ms=$((flink_end_ms - flink_start_ms))
total_ms=$((flink_end_ms - publish_start_ms))
publish_rate="$(awk "BEGIN {printf \"%.2f\", $EXPECTED_MESSAGES / ($publish_ms / 1000)}")"
end_to_end_rate="$(awk "BEGIN {printf \"%.2f\", $EXPECTED_MESSAGES / ($total_ms / 1000)}")"

SUMMARY="$REPORT_DIR/summary.md"
CSV="$REPORT_DIR/summary.csv"

cat >"$SUMMARY" <<EOF
# E2E Benchmark

Pipeline:

\`\`\`text
PythonSimulator -> EMQX MQTT -> EMQX Kafka sink -> Kafka -> Flink bounded Kafka source -> TopKResult
\`\`\`

Config:

- objects: $OBJECTS
- queries: $QUERIES
- dimensions: $DIMENSIONS
- k: $K
- missingRate: $MISSING_RATE
- publisherRatePerSecond: $RATE_PER_SECOND
- mqttQos: $QOS
- flinkParallelism: $PARALLELISM
- synopsisBins: $SYNOPSIS_BINS
- expectedMessages: $EXPECTED_MESSAGES
- kafkaMessages: $kafka_messages
- topKResults: $topk_results
- dataset: $DATASET
- datasetPath: $DATASET_PATH
- topicMappings: $TOPIC_MAPPINGS

Timing:

- mqttPublishMs: $publish_ms
- mqttToKafkaReadyMs: $ingress_ms
- flinkDrainMs: $flink_ms
- totalE2EMs: $total_ms
- publishRateMessagesPerSecond: $publish_rate
- endToEndRateMessagesPerSecond: $end_to_end_rate

Artifacts:

- Flink log: \`reports/e2e/flink.log\`
- Flink submit log: \`reports/e2e/flink-submit.log\`
- CSV: \`reports/e2e/summary.csv\`
EOF

cat >"$CSV" <<EOF
objects,queries,dimensions,k,missing_rate,mqtt_qos,flink_parallelism,synopsis_bins,expected_messages,kafka_messages,topk_results,publish_ms,ingress_ms,flink_ms,total_ms,publish_rate_msg_s,e2e_rate_msg_s,dataset,topic_mappings
$OBJECTS,$QUERIES,$DIMENSIONS,$K,$MISSING_RATE,$QOS,$PARALLELISM,$SYNOPSIS_BINS,$EXPECTED_MESSAGES,$kafka_messages,$topk_results,$publish_ms,$ingress_ms,$flink_ms,$total_ms,$publish_rate,$end_to_end_rate,$DATASET,"$TOPIC_MAPPINGS"
EOF

cat "$SUMMARY"
