#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OBJECTS="${OBJECTS:-200}"
QUERIES="${QUERIES:-2}"
DIMENSIONS="${DIMENSIONS:-4}"
K="${K:-10}"
MISSING_RATE="${MISSING_RATE:-0.35}"
RATE_PER_SECOND="${RATE_PER_SECOND:-200}"
QOS="${QOS:-0}"
WINDOW_MS="${WINDOW_MS:-10000}"
DATASET="${DATASET:-synthetic}"
DATASET_PATH="${DATASET_PATH:-}"
PARTITIONS="${PARTITIONS:-4}"
CANDIDATE_MULTIPLIER="${CANDIDATE_MULTIPLIER:-4}"
PARALLELISM="${PARALLELISM:-1}"
SYNOPSIS_BINS="${SYNOPSIS_BINS:-8}"
MAX_EVENTS="${MAX_EVENTS:-$((OBJECTS * QUERIES))}"
if [[ "$DATASET" == "all" ]]; then
  EXPECTED_MESSAGES="${EXPECTED_MESSAGES:-$((MAX_EVENTS * 3))}"
  MONITOR_ENV=(
    "TOPIC_MAPPINGS=thesis/raw/intel=thesis.raw.intel,thesis/raw/pump=thesis.raw.pump,thesis/raw/gas=thesis.raw.gas"
    "ACTION_IDS=kafka_producer:raw_incomplete_to_kafka_thesis_raw_intel,kafka_producer:raw_incomplete_to_kafka_thesis_raw_pump,kafka_producer:raw_incomplete_to_kafka_thesis_raw_gas"
  )
else
  EXPECTED_MESSAGES="${EXPECTED_MESSAGES:-$((OBJECTS * QUERIES))}"
  MONITOR_ENV=()
fi

cd "$ROOT_DIR"

echo "== python simulator virtual environment and preprocessing =="
scripts/setup-venv.sh
.venv/bin/python scripts/simulator.py --dataset=synthetic --objects=4 --queries=2 --max-events=8 --dry-run --json
.venv/bin/python scripts/simulator.py --dataset=all --max-events=2 --dry-run --json

echo "== unit tests =="
mvn test

echo "== local Flink CLI stream smoke test =="
OBJECTS=4 QUERIES=2 DIMENSIONS=4 K=2 PARALLELISM=2 SYNOPSIS_BINS=4 \
  just run-local >/tmp/thesis-flink-local-cli.log 2>&1
grep -q "pipelineConfig source=simulator parallelism=2" /tmp/thesis-flink-local-cli.log
grep -q "TopKResult{" /tmp/thesis-flink-local-cli.log

echo "== package and docker image =="
mvn -q -DskipTests package
docker build -t thesis-topk:local .
docker run --rm --entrypoint /opt/flink/bin/flink thesis-topk:local --version | grep -q "Version: 2.2.0"
docker build -f docker/simulator/Dockerfile -t thesis-simulator:local .
docker run --rm thesis-simulator:local --dataset=all --max-events=1 --dry-run --json >/tmp/thesis-simulator-image-check.json
python3 -m json.tool /tmp/thesis-simulator-image-check.json >/dev/null

echo "== compose config =="
docker compose -f docker-compose.e2e.yml config >/dev/null

echo "== kubernetes yaml =="
python3 - <<'PY'
from pathlib import Path
import yaml

with Path("k8s/pipeline.yaml").open() as f:
  docs = [doc for doc in yaml.safe_load_all(f) if doc]
if not docs:
  raise SystemExit("k8s/pipeline.yaml contains no resources")
print(f"k8s resources: {len(docs)}")
PY

echo "== algorithm performance benchmark =="
mkdir -p reports/algorithm
mvn -q -DskipTests compile exec:java \
  -Dexec.mainClass=com.thesis.topk.benchmark.TopKBenchmark \
  -Dexec.args="--dataset=$DATASET --datasetPath=$DATASET_PATH --objects=$OBJECTS --dimensions=$DIMENSIONS --queries=$QUERIES --k=$K --missingRate=$MISSING_RATE --seed=7 --partitions=$PARTITIONS --candidateMultiplier=$CANDIDATE_MULTIPLIER --synopsisBins=$SYNOPSIS_BINS" \
  > reports/algorithm/topk-${OBJECTS}x${QUERIES}.txt
tail -n 8 reports/algorithm/topk-${OBJECTS}x${QUERIES}.txt

echo "== full e2e benchmark =="
OBJECTS="$OBJECTS" \
QUERIES="$QUERIES" \
DIMENSIONS="$DIMENSIONS" \
K="$K" \
MISSING_RATE="$MISSING_RATE" \
RATE_PER_SECOND="$RATE_PER_SECOND" \
QOS="$QOS" \
WINDOW_MS="$WINDOW_MS" \
DATASET="$DATASET" \
DATASET_PATH="$DATASET_PATH" \
MAX_EVENTS="$MAX_EVENTS" \
EXPECTED_MESSAGES="$EXPECTED_MESSAGES" \
PARALLELISM="$PARALLELISM" \
SYNOPSIS_BINS="$SYNOPSIS_BINS" \
BUILD_IMAGE=0 \
scripts/e2e-benchmark.sh

echo "== monitor cli assertions =="
env "${MONITOR_ENV[@]}" EXPECTED_MESSAGES="$EXPECTED_MESSAGES" scripts/test-monitor-cli.sh

echo "== strict e2e artifact and live-state validation =="
env "${MONITOR_ENV[@]}" python3 scripts/validate-e2e.py \
  --expected-messages "$EXPECTED_MESSAGES" \
  --expected-queries "$QUERIES"

echo "== gui http smoke test =="
if ! curl -fsS http://localhost:8088/api/tests/status >/dev/null 2>&1; then
  if [[ -f /tmp/thesis-monitor.pid ]]; then
    kill "$(cat /tmp/thesis-monitor.pid)" >/dev/null 2>&1 || true
  fi
  fuser -k 8088/tcp >/dev/null 2>&1 || true
  setsid env PORT=8088 scripts/monitor.sh >/tmp/thesis-monitor.log 2>&1 < /dev/null &
  echo $! >/tmp/thesis-monitor.pid
  sleep 1
fi
curl -fsS http://localhost:8088/ >/tmp/thesis-monitor.html
grep -q "Thesis Stream Monitor" /tmp/thesis-monitor.html
grep -q "Imputation MAE" /tmp/thesis-monitor.html
grep -q "DD Synopsis Rules" /tmp/thesis-monitor.html
curl -fsS http://localhost:8088/api/metrics >/tmp/thesis-monitor-metrics.json
python3 -m json.tool /tmp/thesis-monitor-metrics.json >/dev/null
python3 - <<'PY'
import json

with open("/tmp/thesis-monitor-metrics.json") as f:
  payload = json.load(f)
required = ["mqtt", "kafka", "flink", "accuracy", "issues"]
missing = [key for key in required if key not in payload]
if missing:
  raise SystemExit(f"missing monitor JSON keys: {missing}")
for key in ["synopsisRules", "imputationHoldoutMae"]:
  if payload["accuracy"].get(key) is None:
    raise SystemExit(f"missing monitor accuracy metric: {key}")
PY
curl -fsS http://localhost:8088/api/tests/status | python3 -m json.tool >/dev/null

echo "all tests passed"
