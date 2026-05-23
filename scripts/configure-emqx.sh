#!/usr/bin/env bash
set -euo pipefail

EMQX_URL="${EMQX_URL:-http://localhost:18084}"
EMQX_USER="${EMQX_USER:-admin}"
EMQX_PASSWORD="${EMQX_PASSWORD:-public}"
KAFKA_BOOTSTRAP_INTERNAL="${KAFKA_BOOTSTRAP_INTERNAL:-kafka:9092}"
KAFKA_TOPIC="${KAFKA_TOPIC:-thesis.raw.incomplete}"
MQTT_TOPIC="${MQTT_TOPIC:-thesis/raw}"
TOPIC_MAPPINGS="${TOPIC_MAPPINGS:-$MQTT_TOPIC=$KAFKA_TOPIC}"
CONNECTOR_NAME="${CONNECTOR_NAME:-kafka_ingress}"
ACTION_NAME_PREFIX="${ACTION_NAME_PREFIX:-raw_incomplete_to_kafka}"
RULE_ID_PREFIX="${RULE_ID_PREFIX:-mqtt_raw_to_kafka}"

need() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Missing required command: $1" >&2
    exit 1
  }
}

need curl
need jq

login() {
  curl -fsS -X POST "$EMQX_URL/api/v5/login" \
    -H 'content-type: application/json' \
    -d "{\"username\":\"$EMQX_USER\",\"password\":\"$EMQX_PASSWORD\"}" \
    | jq -r '.token'
}

TOKEN="$(login)"
AUTH=(-H "Authorization: Bearer $TOKEN" -H "content-type: application/json")

api() {
  local method="$1"
  local path="$2"
  local data="${3:-}"
  if [[ -n "$data" ]]; then
    curl -fsS -X "$method" "$EMQX_URL$path" "${AUTH[@]}" -d "$data"
  else
    curl -fsS -X "$method" "$EMQX_URL$path" "${AUTH[@]}"
  fi
}

delete_if_exists() {
  local path="$1"
  curl -fsS -X DELETE "$EMQX_URL$path" "${AUTH[@]}" >/dev/null 2>&1 || true
}

echo "Configuring EMQX Kafka ingress via HTTP API at $EMQX_URL"

slug() {
  echo "$1" | tr '/.' '__' | tr -cd '[:alnum:]_'
}

IFS=',' read -ra mappings <<<"$TOPIC_MAPPINGS"
for mapping in "${mappings[@]}"; do
  mqtt="${mapping%%=*}"
  kafka="${mapping#*=}"
  suffix="$(slug "$mqtt")"
  action_name="${ACTION_NAME_PREFIX}_${suffix}"
  rule_id="${RULE_ID_PREFIX}_${suffix}"

  delete_if_exists "/api/v5/rules/$rule_id"
  delete_if_exists "/api/v5/actions/kafka_producer:$action_name"
done

delete_if_exists "/api/v5/connectors/kafka_producer:$CONNECTOR_NAME"

connector_payload="$(jq -n \
  --arg name "$CONNECTOR_NAME" \
  --arg bootstrap "$KAFKA_BOOTSTRAP_INTERNAL" \
  '{
    type: "kafka_producer",
    name: $name,
    enable: true,
    bootstrap_hosts: $bootstrap,
    connect_timeout: "5s",
    metadata_request_timeout: "5s",
    min_metadata_refresh_interval: "3s"
  }')"

api POST /api/v5/connectors "$connector_payload" >/dev/null

echo "Configured:"
api GET "/api/v5/connectors/kafka_producer:$CONNECTOR_NAME" \
  | jq '{connector: .name, type: .type, status: .status, bootstrap_hosts: .bootstrap_hosts}'

for mapping in "${mappings[@]}"; do
  mqtt="${mapping%%=*}"
  kafka="${mapping#*=}"
  suffix="$(slug "$mqtt")"
  action_name="${ACTION_NAME_PREFIX}_${suffix}"
  rule_id="${RULE_ID_PREFIX}_${suffix}"

  action_payload="$(jq -n \
    --arg name "$action_name" \
    --arg connector "$CONNECTOR_NAME" \
    --arg topic "$kafka" \
    '{
      type: "kafka_producer",
      name: $name,
      enable: true,
      connector: $connector,
      parameters: {
        topic: $topic,
        message: {
          key: "${clientid}",
          value: "${payload}"
        },
        partition_strategy: "random",
        required_acks: "all_isr",
        query_mode: "async"
      }
    }')"

  rule_payload="$(jq -n \
    --arg sql "SELECT payload, clientid, topic, timestamp FROM \"$mqtt\"" \
    --arg action "kafka_producer:$action_name" \
    '{
      enable: true,
      sql: $sql,
      actions: [$action],
      description: "Forward incomplete MQTT stream records to Kafka"
    }')"

  api POST /api/v5/actions "$action_payload" >/dev/null
  api PUT "/api/v5/rules/$rule_id" "$rule_payload" >/dev/null

  api GET "/api/v5/actions/kafka_producer:$action_name" \
    | jq '{action: .name, type: .type, status: .status, topic: .parameters.topic, connector: .connector}'
  api GET "/api/v5/rules/$rule_id" \
    | jq '{rule: .id, enable: .enable, sql: .sql, actions: .actions}'
done
