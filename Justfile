set dotenv-load

compose_file := "docker-compose.e2e.yml"
image := "thesis-topk:local"
simulator_image := "thesis-simulator:local"
objects := env_var_or_default("OBJECTS", "200")
queries := env_var_or_default("QUERIES", "2")
dimensions := env_var_or_default("DIMENSIONS", "4")
k := env_var_or_default("K", "10")
missing_rate := env_var_or_default("MISSING_RATE", "0.35")
rate_per_second := env_var_or_default("RATE_PER_SECOND", "200")
qos := env_var_or_default("QOS", "0")
window_ms := env_var_or_default("WINDOW_MS", "10000")
expected_messages := env_var_or_default("EXPECTED_MESSAGES", "400")
monitor_port := env_var_or_default("PORT", "8088")
dataset := env_var_or_default("DATASET", "synthetic")
dataset_path := env_var_or_default("DATASET_PATH", "")
partitions := env_var_or_default("PARTITIONS", "4")
candidate_multiplier := env_var_or_default("CANDIDATE_MULTIPLIER", "4")
parallelism := env_var_or_default("PARALLELISM", "1")
synopsis_bins := env_var_or_default("SYNOPSIS_BINS", "8")
max_events := env_var_or_default("MAX_EVENTS", "0")

# list available recipes
default:
    just --list

# run Java unit tests
test:
    mvn test

# create the isolated Python simulator environment
venv:
    scripts/setup-venv.sh

# validate all Python simulator preprocessors without publishing
simulator-test: venv
    .venv/bin/python scripts/simulator.py --dataset=synthetic --objects=4 --queries=2 --max-events=8 --dry-run --json
    .venv/bin/python scripts/simulator.py --dataset=all --max-events=2 --dry-run --json

# package the shaded application jar
package:
    mvn -q -DskipTests package

# build the Apache Flink based runtime image
image: package
    docker build -t {{ image }} .

# build the decoupled Python simulator image for Kubernetes
simulator-image:
    docker build -f docker/simulator/Dockerfile -t {{ simulator_image }} .

# verify the runtime image contains Apache Flink 2.2.0
image-check: image
    docker run --rm --entrypoint /opt/flink/bin/flink {{ image }} --version | grep -q "Version: 2.2.0"

# validate docker compose and Kubernetes manifests
config-check:
    docker compose -f {{ compose_file }} config >/dev/null
    python3 -c 'from pathlib import Path; import yaml; docs=[doc for doc in yaml.safe_load_all(Path("k8s/pipeline.yaml").read_text()) if doc]; assert docs, "k8s/pipeline.yaml contains no resources"; print(f"k8s resources: {len(docs)}")'

# run the algorithm-only performance benchmark
bench:
    mkdir -p reports/algorithm
    mvn -q -DskipTests compile exec:java \
      -Dexec.mainClass=com.thesis.topk.benchmark.TopKBenchmark \
      -Dexec.args="--dataset={{ dataset }} --datasetPath={{ dataset_path }} --objects={{ objects }} --dimensions={{ dimensions }} --queries={{ queries }} --k={{ k }} --missingRate={{ missing_rate }} --seed=7 --partitions={{ partitions }} --candidateMultiplier={{ candidate_multiplier }} --synopsisBins={{ synopsis_bins }}" \
      > reports/algorithm/topk-{{ objects }}x{{ queries }}.txt
    tail -n 8 reports/algorithm/topk-{{ objects }}x{{ queries }}.txt

# run the simulator-backed Flink job on the local JVM with provided Flink libraries
run-local:
    JAVA_TOOL_OPTIONS="--add-opens=java.base/java.util=ALL-UNNAMED" \
      mvn -q -DskipTests compile exec:exec -Dexec.classpathScope=compile -Dexec.executable=java \
      -Dexec.args="-cp %classpath com.thesis.topk.flink.ProbabilisticTopKJob --dataset={{ dataset }} --datasetPath={{ dataset_path }} --objects={{ objects }} --dimensions={{ dimensions }} --queries={{ queries }} --k={{ k }} --windowMs={{ window_ms }} --parallelism={{ parallelism }} --synopsisBins={{ synopsis_bins }}"

# publish generated incomplete records to a local MQTT broker
publish-local:
    just venv
    .venv/bin/python scripts/simulator.py \
      --dataset={{ dataset }} --dataset-path={{ dataset_path }} --mqtt-host=localhost --mqtt-port=1883 --topic=thesis/raw \
      --objects={{ objects }} --dimensions={{ dimensions }} --queries={{ queries }} --missing-rate={{ missing_rate }} \
      --rate-per-second=20 --repeat=1

# run the Kafka-backed Flink job on the local JVM with provided Flink libraries
run-kafka-local:
    JAVA_TOOL_OPTIONS="--add-opens=java.base/java.util=ALL-UNNAMED" \
      mvn -q -DskipTests compile exec:exec -Dexec.classpathScope=compile -Dexec.executable=java \
      -Dexec.args="-cp %classpath com.thesis.topk.flink.ProbabilisticTopKJob --source=kafka --kafkaBootstrap=localhost:9092 --kafkaTopic=thesis.raw.incomplete --dataset={{ dataset }} --datasetPath={{ dataset_path }} --objects={{ objects }} --dimensions={{ dimensions }} --queries={{ queries }} --k={{ k }} --windowMs={{ window_ms }} --parallelism={{ parallelism }} --synopsisBins={{ synopsis_bins }}"

# start Kafka, EMQX, and the Flink session cluster
setup: image
    RESET=1 scripts/setup-services.sh

# start services without rebuilding the image
setup-fast:
    BUILD_IMAGE=0 RESET=1 scripts/setup-services.sh

# run the full Dockerized E2E benchmark
e2e:
    OBJECTS={{ objects }} \
    QUERIES={{ queries }} \
    DIMENSIONS={{ dimensions }} \
    K={{ k }} \
    MISSING_RATE={{ missing_rate }} \
    RATE_PER_SECOND={{ rate_per_second }} \
    QOS={{ qos }} \
    WINDOW_MS={{ window_ms }} \
    DATASET={{ dataset }} \
    DATASET_PATH={{ dataset_path }} \
    MAX_EVENTS={{ max_events }} \
    PARALLELISM={{ parallelism }} \
    SYNOPSIS_BINS={{ synopsis_bins }} \
    scripts/e2e-benchmark.sh

# run E2E without rebuilding the image in setup-services
e2e-fast:
    OBJECTS={{ objects }} \
    QUERIES={{ queries }} \
    DIMENSIONS={{ dimensions }} \
    K={{ k }} \
    MISSING_RATE={{ missing_rate }} \
    RATE_PER_SECOND={{ rate_per_second }} \
    QOS={{ qos }} \
    WINDOW_MS={{ window_ms }} \
    DATASET={{ dataset }} \
    DATASET_PATH={{ dataset_path }} \
    MAX_EVENTS={{ max_events }} \
    PARALLELISM={{ parallelism }} \
    SYNOPSIS_BINS={{ synopsis_bins }} \
    BUILD_IMAGE=0 \
    scripts/e2e-benchmark.sh

# submit the bounded Kafka job to the Dockerized Flink session cluster
flink-submit:
    docker compose -f {{ compose_file }} exec -T flink-jobmanager \
      /opt/flink/bin/flink run -m flink-jobmanager:8081 \
      -c com.thesis.topk.flink.ProbabilisticTopKJob \
      /opt/flink/usrlib/topk.jar \
      --source=kafka --kafkaBounded=true --kafkaBootstrap=kafka:9092 \
      --kafkaTopic=thesis.raw.incomplete --dataset={{ dataset }} --datasetPath={{ dataset_path }} --objects={{ objects }} --dimensions={{ dimensions }} --queries={{ queries }} --k={{ k }} --parallelism={{ parallelism }} --synopsisBins={{ synopsis_bins }}

# validate monitor metrics from the CLI
monitor-check expected=expected_messages:
    EXPECTED_MESSAGES={{ expected }} scripts/test-monitor-cli.sh

# print one monitor metrics sample as JSON
monitor-json:
    python3 scripts/monitor.py --once --json

# start the realtime monitor GUI
monitor:
    PORT={{ monitor_port }} scripts/monitor.sh

# start the realtime monitor GUI in the background
monitor-bg:
    if [ -f /tmp/thesis-monitor.pid ]; then kill "$(cat /tmp/thesis-monitor.pid)" >/dev/null 2>&1 || true; fi; \
      fuser -k {{ monitor_port }}/tcp >/dev/null 2>&1 || true; \
      setsid env PORT={{ monitor_port }} scripts/monitor.sh >/tmp/thesis-monitor.log 2>&1 < /dev/null & \
      echo $! >/tmp/thesis-monitor.pid; \
      sleep 1; \
      echo "monitor=http://localhost:{{ monitor_port }}"

# strictly validate latest E2E artifacts and live state
validate expected=expected_messages:
    python3 scripts/validate-e2e.py --expected-messages {{ expected }} --expected-queries {{ queries }}

# show the paper-alignment validation report
paper-report:
    sed -n '1,220p' reports/validation/paper-alignment.md

# run all CLI tests and benchmarks
test-all:
    OBJECTS={{ objects }} \
    QUERIES={{ queries }} \
    DIMENSIONS={{ dimensions }} \
    K={{ k }} \
    MISSING_RATE={{ missing_rate }} \
    RATE_PER_SECOND={{ rate_per_second }} \
    QOS={{ qos }} \
    WINDOW_MS={{ window_ms }} \
    DATASET={{ dataset }} \
    DATASET_PATH={{ dataset_path }} \
    PARTITIONS={{ partitions }} \
    CANDIDATE_MULTIPLIER={{ candidate_multiplier }} \
    PARALLELISM={{ parallelism }} \
    SYNOPSIS_BINS={{ synopsis_bins }} \
    scripts/test-all.sh

# run full verification and preserve verbose console output for the GUI
test-all-verbose:
    mkdir -p reports/tests
    set -o pipefail; OBJECTS={{ objects }} QUERIES={{ queries }} DIMENSIONS={{ dimensions }} K={{ k }} \
      MISSING_RATE={{ missing_rate }} RATE_PER_SECOND={{ rate_per_second }} QOS={{ qos }} WINDOW_MS={{ window_ms }} \
      DATASET={{ dataset }} DATASET_PATH={{ dataset_path }} PARTITIONS={{ partitions }} CANDIDATE_MULTIPLIER={{ candidate_multiplier }} \
      PARALLELISM={{ parallelism }} SYNOPSIS_BINS={{ synopsis_bins }} \
      scripts/test-all.sh 2>&1 | tee reports/tests/latest.log

# show current Docker services
ps:
    docker compose -f {{ compose_file }} ps

# show Flink REST overview
flink:
    curl -fsS http://localhost:8081/overview | python3 -m json.tool

# show EMQX connector/action/rule metrics
metrics:
    python3 scripts/monitor.py --once

# stop and remove Docker E2E services and volumes
clean:
    docker compose -f {{ compose_file }} down -v --remove-orphans

# apply Kubernetes manifest
k8s-apply:
    kubectl apply -f k8s/pipeline.yaml

# show Kubernetes pods
k8s-pods:
    kubectl -n thesis-streaming get pods

# follow Flink JobManager logs in Kubernetes
k8s-logs:
    kubectl -n thesis-streaming logs job/flink-jobmanager -f
