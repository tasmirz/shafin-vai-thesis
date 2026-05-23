#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-$ROOT_DIR/docker-compose.e2e.yml}"
KAFKA_TOPIC="${KAFKA_TOPIC:-thesis.raw.incomplete}"
TOPIC_MAPPINGS="${TOPIC_MAPPINGS:-thesis/raw=$KAFKA_TOPIC}"
RESET="${RESET:-1}"
BUILD_IMAGE="${BUILD_IMAGE:-1}"

need() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Missing required command: $1" >&2
    exit 1
  }
}

need docker
need curl
need jq

wait_for_tcp() {
  local host="$1"
  local port="$2"
  local deadline=$((SECONDS + 120))
  until (echo >"/dev/tcp/$host/$port") >/dev/null 2>&1; do
    if ((SECONDS >= deadline)); then
      echo "Timed out waiting for $host:$port" >&2
      return 1
    fi
    sleep 1
  done
}

wait_for_kafka() {
  local deadline=$((SECONDS + 180))
  until docker compose -f "$COMPOSE_FILE" exec -T kafka \
    /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list >/dev/null 2>&1; do
    if ((SECONDS >= deadline)); then
      echo "Timed out waiting for Kafka" >&2
      return 1
    fi
    sleep 2
  done
}

wait_for_emqx() {
  local deadline=$((SECONDS + 120))
  until curl -fsS http://localhost:18084/api/v5/status >/dev/null 2>&1; do
    if ((SECONDS >= deadline)); then
      echo "Timed out waiting for EMQX dashboard API" >&2
      return 1
    fi
    sleep 1
  done
}

wait_for_flink() {
  local deadline=$((SECONDS + 180))
  until curl -fsS http://localhost:8081/overview >/dev/null 2>&1; do
    if ((SECONDS >= deadline)); then
      echo "Timed out waiting for Flink JobManager" >&2
      return 1
    fi
    sleep 2
  done
}

cd "$ROOT_DIR"

if [[ "$BUILD_IMAGE" == "1" || "$BUILD_IMAGE" == "true" ]]; then
  mvn -q -DskipTests package
  docker build -t thesis-topk:local "$ROOT_DIR"
fi

if [[ "$RESET" == "1" || "$RESET" == "true" ]]; then
  docker compose -f "$COMPOSE_FILE" down -v --remove-orphans >/dev/null 2>&1 || true
fi

docker compose -f "$COMPOSE_FILE" up -d kafka emqx flink-jobmanager flink-taskmanager
wait_for_kafka
wait_for_tcp 127.0.0.1 18884
wait_for_emqx
wait_for_flink

IFS=',' read -ra mappings <<<"$TOPIC_MAPPINGS"
for mapping in "${mappings[@]}"; do
  topic="${mapping#*=}"
  docker compose -f "$COMPOSE_FILE" exec -T kafka \
    /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:9092 \
    --create \
    --if-not-exists \
    --topic "$topic" \
    --partitions 1 \
    --replication-factor 1 >/dev/null
done

TOPIC_MAPPINGS="$TOPIC_MAPPINGS" "$ROOT_DIR/scripts/configure-emqx.sh"

echo "Services are ready:"
docker compose -f "$COMPOSE_FILE" ps
