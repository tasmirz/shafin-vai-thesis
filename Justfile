set dotenv-load

compose_file := "docker-compose.e2e.yml"
hadoop_compose_file := "docker-compose.hadoop.yml"
image := "thesis-topk-spark:local"
simulator_image := "thesis-simulator:local"
objects := env_var_or_default("OBJECTS", "200")
queries := env_var_or_default("QUERIES", "2")
dimensions := env_var_or_default("DIMENSIONS", "4")
k := env_var_or_default("K", "10")
missing_rate := env_var_or_default("MISSING_RATE", "0.35")
rate_per_second := env_var_or_default("RATE_PER_SECOND", "200")
qos := env_var_or_default("QOS", "1")
expected_messages := env_var_or_default("EXPECTED_MESSAGES", "400")
monitor_port := env_var_or_default("PORT", "8088")
web_port := env_var_or_default("WEB_PORT", "8090")
dataset := env_var_or_default("DATASET", "synthetic")
dataset_path := env_var_or_default("DATASET_PATH", "")
partitions := env_var_or_default("PARTITIONS", "4")
candidate_multiplier := env_var_or_default("CANDIDATE_MULTIPLIER", "4")
synopsis_bins := env_var_or_default("SYNOPSIS_BINS", "8")
max_events := env_var_or_default("MAX_EVENTS", "0")
run_id := env_var_or_default("RUN_ID", "")
algorithm := env_var_or_default("ALGORITHM", "aes-dscp")

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

# build the Apache Spark runtime image
image: package
    docker build -t {{ image }} .

# alias for Spark image builds
spark-image: image

# build the decoupled Python simulator image for Kubernetes
simulator-image:
    docker build -f docker/simulator/Dockerfile -t {{ simulator_image }} .

# verify the runtime image contains Spark and the application jar
image-check: image
    docker run --rm {{ image }} /opt/spark/bin/spark-submit --version 2>&1 | grep -qi "spark"
    docker run --rm {{ image }} test -f /opt/spark/app/topk-spark.jar

# validate docker compose and Kubernetes manifests
config-check:
    docker compose -f {{ compose_file }} config >/dev/null
    docker compose -f {{ hadoop_compose_file }} config >/dev/null
    python3 -c 'from pathlib import Path; import yaml; docs=[doc for doc in yaml.safe_load_all(Path("k8s/pipeline.yaml").read_text()) if doc]; assert docs, "k8s/pipeline.yaml contains no resources"; names=[doc.get("metadata",{}).get("name","") for doc in docs]; assert "spark-master" in names and "spark-topk-submit" in names; print(f"k8s resources: {len(docs)}")'

# validate isolated HDFS/YARN MapReduce infrastructure; not a PTD algorithm benchmark
hadoop-smoke:
    scripts/research/validate_hadoop_cluster.sh

# run one executable Hadoop MapReduce PTD treatment inside the Docker Hadoop cluster
hadoop-csv-test:
    RUN_ID={{ run_id }} ALGORITHM={{ algorithm }} BUILD_IMAGE="${BUILD_IMAGE:-1}" scripts/research/run_hadoop_csv_benchmark.sh

# run executable Hadoop MapReduce baseline/AES/DSCP/AES+DSCP treatments
hadoop-ablation-test:
    SUITE_ID={{ run_id }} BUILD_IMAGE="${BUILD_IMAGE:-1}" scripts/research/run_hadoop_ablation_suite.sh

# run the Spark upgraded job locally
spark:
    mkdir -p reports/spark
    report_path="${SPARK_REPORT_PATH:-reports/spark/topk-spark-{{ objects }}x{{ queries }}.txt}"; \
      mvn -q -DskipTests package && \
      docker build -t thesis-topk-spark:local . >/dev/null && \
      docker run --rm \
      -v $(pwd)/reports:/opt/spark/work-dir/reports \
      -v $(pwd)/datasets-raw:/opt/spark/datasets-raw:ro \
      thesis-topk-spark:local \
      /opt/spark/bin/spark-submit \
      --master "local[*]" \
      --class com.thesis.topk.spark.ProbabilisticTopKSparkJob \
      /opt/spark/app/topk-spark.jar \
      --source=simulator --dataset={{ dataset }} --datasetPath={{ dataset_path }} --objects={{ objects }} --dimensions={{ dimensions }} --queries={{ queries }} --k={{ k }} --missingRate={{ missing_rate }} --seed=7 --partitions={{ partitions }} --synopsisBins={{ synopsis_bins }} --algorithm={{ algorithm }} --sparkMaster=local[*] \
      > "$report_path" && \
      tail -n 12 "$report_path"

# run the algorithm-only performance benchmark
bench:
    mkdir -p reports/algorithm
    mvn -q -DskipTests compile exec:java \
      -Dexec.mainClass=com.thesis.topk.benchmark.TopKBenchmark \
      -Dexec.args="--dataset={{ dataset }} --datasetPath={{ dataset_path }} --objects={{ objects }} --dimensions={{ dimensions }} --queries={{ queries }} --k={{ k }} --missingRate={{ missing_rate }} --seed=7 --partitions={{ partitions }} --candidateMultiplier={{ candidate_multiplier }} --synopsisBins={{ synopsis_bins }}" \
      > reports/algorithm/topk-{{ objects }}x{{ queries }}.txt
    tail -n 8 reports/algorithm/topk-{{ objects }}x{{ queries }}.txt

# run the deterministic CSV-to-Spark research integration profile and save its artifacts
csv-test:
    RUN_ID={{ run_id }} ALGORITHM={{ algorithm }} tests/integration/test_csv_spark.sh

# execute and compare all four paper ablation treatments on the deterministic CSV profile
ablation-test:
    SUITE_ID={{ run_id }} BUILD_IMAGE="${BUILD_IMAGE:-1}" scripts/research/run_ablation_suite.sh

# run the full three-way comparison (Spark Baseline vs New Spark vs ICCIT Hadoop)
three-way-compare:
    scripts/research/run_three_way_comparison.sh

# run the four ICCIT treatments against a curated profile and create a comparison report
iccit-compare:
    PROFILE="${PROFILE:-smartphone}" SUITE_ID={{ run_id }} K="${K:-10}" PARTITIONS="${PARTITIONS:-8}" \
      VALIDATE_EXACT="${VALIDATE_EXACT:-false}" BUILD_IMAGE="${BUILD_IMAGE:-1}" \
      scripts/research/run_iccit_comparison_suite.sh

# run a named curated paper setup: rai-baseline, iccit-upgrade, paired, or ablation
paper-setup:
    PROFILE="${PROFILE:-smartphone}" SETUP="${SETUP:-paired}" SUITE_ID={{ run_id }} K="${K:-10}" PARTITIONS="${PARTITIONS:-8}" \
      VALIDATE_EXACT="${VALIDATE_EXACT:-false}" BUILD_IMAGE="${BUILD_IMAGE:-1}" \
      scripts/research/run_paper_setup.sh

# validate the Hadoop-reference AES+DSCP comparison report on a small fixture
hadoop-aes-dscp-test:
    SUITE_ID={{ run_id }} BUILD_IMAGE="${BUILD_IMAGE:-1}" tests/integration/test_hadoop_aes_dscp_comparison.sh

# render ICCIT-shaped observed Spark figures from completed smartphone and road suites
paper-figures smartphone_suite road_suite:
    python3 scripts/research/render_paper_figures.py --smartphone-suite {{ smartphone_suite }} --road-suite {{ road_suite }}

# generate the publication-formatted Spark improvement and source-paper consistency report
publication-report smartphone_suite="iccit-smartphone-str-20260527T073310Z" road_suite="iccit-road-full-20q-20260527T094500Z":
    python3 scripts/research/render_paper_figures.py --smartphone-suite {{ smartphone_suite }} --road-suite {{ road_suite }}
    python3 scripts/research/render_publication_report.py --smartphone-suite {{ smartphone_suite }} --road-suite {{ road_suite }}

# validate locally supplied Bangladesh OSM roads against the paper curation protocol
osm-prepare-check:
    tests/integration/test_osm_preparation.sh

# validate normalized smartphone, Rai-Lian synthetic and EPSG:9678 road-MBR dataset generation
paper-datasets-test:
    tests/integration/test_paper_datasets.sh

# build a Rai-Lian synthetic uncertain-region artifact; set DISTRIBUTION/lmax through the shell
paper-synthetic:
    mkdir -p datasets-curated reports/datasets
    python3 scripts/research/build_paper_dataset.py synthetic --distribution "${DISTRIBUTION:-uniform}" --lmax "${LMAX:-10}" --zipf-skew 0.8 --output "datasets-curated/rai-${DISTRIBUTION:-uniform}.csv" --manifest "reports/datasets/rai-${DISTRIBUTION:-uniform}.json" --objects "${OBJECTS:-100000}" --instances-min 2 --instances-max 10 --queries "${QUERIES:-1}" --partitions "${PARTITIONS:-8}" --seed 42

# build the paper-shaped smartphone uncertain-object artifact and manifest
paper-smartphone:
    mkdir -p datasets-curated reports/datasets
    python3 scripts/research/build_paper_dataset.py smartphone --output datasets-curated/smartphone-paper.csv --manifest reports/datasets/smartphone-paper.json --objects 750 --instances-min 8 --instances-max 20 --queries 20 --partitions 8 --seed 42

# curate a baseline-scale Bangladesh road-MBR uncertain-object artifact and manifest
paper-osm:
    mkdir -p datasets-curated reports/datasets
    uv run --with pyproj scripts/research/build_paper_dataset.py osm --output datasets-curated/bangladesh-road-paper.csv --manifest reports/datasets/bangladesh-road-paper.json --objects 98451 --instances-min 5 --instances-max 11 --queries 1 --partitions 8 --seed 42

# curate the supplied Los Angeles County TIGER road source at the Rai-Lian real-data scale
paper-california:
    mkdir -p datasets-curated reports/datasets
    uv run --with pyproj scripts/research/build_paper_dataset.py osm --source datasets-osm/tl_2018_06037_roads/tl_2018_06037_roads.shp --dataset-name california-tiger-la-road-mbr --source-crs EPSG:4269 --projected-crs EPSG:3310 --output datasets-curated/california-tiger-road-paper.csv --manifest reports/datasets/california-tiger-road-paper.json --objects 98451 --instances-min 5 --instances-max 11 --queries 1 --partitions 8 --seed 42
    python3 scripts/research/build_query_set.py --dataset-manifest reports/datasets/california-tiger-road-paper.json --output datasets-curated/california-tiger-road-queries-20.csv --manifest reports/datasets/california-tiger-road-queries-20.json --queries 20 --seed 42

# compile selected immutable CSV treatment suites and stream runs into one report
dataset-benchmark-report *args:
    python3 scripts/research/render_dataset_benchmark_report.py {{ args }}

# run MQTT -> Kafka -> Spark Structured Streaming E2E and save its artifacts
stream-test:
    RUN_ID={{ run_id }} ALGORITHM={{ algorithm }} OBJECTS={{ objects }} QUERIES={{ queries }} DIMENSIONS={{ dimensions }} K={{ k }} \
      PARTITIONS={{ partitions }} RATE_PER_SECOND={{ rate_per_second }} tests/e2e/test_mqtt_kafka_spark.sh

# compare saved research runs: just compare-runs run-a run-b
compare-runs first second:
    python3 scripts/research/compare_runs.py {{ first }} {{ second }}

# serve the PTD-BenchLab research workbench website
web:
    python3 web/server.py --port {{ web_port }}

# validate website artifact parsing and comparison behavior
web-test:
    python3 -m unittest discover -s tests/web -p 'test_*.py'

# exercise the local PTD-BenchLab website through HTTP from the CLI
web-smoke-test:
    tests/web/test_http.sh

# run the simulator-backed Spark job on the local JVM
run-local: spark

# publish generated incomplete records to a local MQTT broker
publish-local:
    just venv
    .venv/bin/python scripts/simulator.py \
      --dataset={{ dataset }} --dataset-path={{ dataset_path }} --mqtt-host=localhost --mqtt-port=1883 --topic=thesis/raw \
      --objects={{ objects }} --dimensions={{ dimensions }} --queries={{ queries }} --missing-rate={{ missing_rate }} \
      --rate-per-second=20 --repeat=1

# run the Kafka-backed Spark job on the local JVM
run-kafka-local:
    mvn -q -DskipTests package
    docker build -t thesis-topk-spark:local . >/dev/null
    docker run --rm \
      --net=host \
      -v $(pwd)/datasets-raw:/opt/spark/datasets-raw:ro \
      thesis-topk-spark:local \
      /opt/spark/bin/spark-submit \
      --master "local[*]" \
      --class com.thesis.topk.spark.ProbabilisticTopKSparkJob \
      /opt/spark/app/topk-spark.jar \
      --source=kafka --kafkaBootstrap=localhost:29092 --kafkaTopic=thesis.raw.incomplete --expectedMessages={{ expected_messages }} --dataset={{ dataset }} --datasetPath={{ dataset_path }} --objects={{ objects }} --dimensions={{ dimensions }} --queries={{ queries }} --k={{ k }} --missingRate={{ missing_rate }} --partitions={{ partitions }} --synopsisBins={{ synopsis_bins }} --algorithm={{ algorithm }} --sparkMaster=local[*]

# start Kafka, EMQX, and the Spark standalone cluster
setup: image
    RESET=1 scripts/setup-services.sh

# start services without rebuilding the image
setup-fast:
    BUILD_IMAGE=0 RESET=1 scripts/setup-services.sh

# run the full Dockerized Spark E2E benchmark
e2e:
    OBJECTS={{ objects }} \
    QUERIES={{ queries }} \
    DIMENSIONS={{ dimensions }} \
    K={{ k }} \
    MISSING_RATE={{ missing_rate }} \
    RATE_PER_SECOND={{ rate_per_second }} \
    QOS={{ qos }} \
    DATASET={{ dataset }} \
    DATASET_PATH={{ dataset_path }} \
    MAX_EVENTS={{ max_events }} \
    PARTITIONS={{ partitions }} \
    SYNOPSIS_BINS={{ synopsis_bins }} \
    ALGORITHM={{ algorithm }} \
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
    DATASET={{ dataset }} \
    DATASET_PATH={{ dataset_path }} \
    MAX_EVENTS={{ max_events }} \
    PARTITIONS={{ partitions }} \
    SYNOPSIS_BINS={{ synopsis_bins }} \
    ALGORITHM={{ algorithm }} \
    BUILD_IMAGE=0 \
    scripts/e2e-benchmark.sh

# submit the bounded Kafka job to the Dockerized Spark standalone cluster
spark-submit:
    docker compose -f {{ compose_file }} run --rm spark-submit \
      /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 \
      --deploy-mode client \
      --class com.thesis.topk.spark.ProbabilisticTopKSparkJob \
      /opt/spark/app/topk-spark.jar \
      --source=kafka --kafkaBootstrap=kafka:9092 --kafkaTopic=thesis.raw.incomplete \
      --expectedMessages={{ expected_messages }} --dataset={{ dataset }} --datasetPath={{ dataset_path }} \
      --objects={{ objects }} --dimensions={{ dimensions }} --queries={{ queries }} --k={{ k }} \
      --missingRate={{ missing_rate }} --partitions={{ partitions }} --synopsisBins={{ synopsis_bins }} --algorithm={{ algorithm }} \
      --sparkMaster=spark://spark-master:7077

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
    DATASET={{ dataset }} \
    DATASET_PATH={{ dataset_path }} \
    PARTITIONS={{ partitions }} \
    CANDIDATE_MULTIPLIER={{ candidate_multiplier }} \
    SYNOPSIS_BINS={{ synopsis_bins }} \
    scripts/test-all.sh

# run full verification and preserve verbose console output for the GUI
test-all-verbose:
    mkdir -p reports/tests
    set -o pipefail; OBJECTS={{ objects }} QUERIES={{ queries }} DIMENSIONS={{ dimensions }} K={{ k }} \
      MISSING_RATE={{ missing_rate }} RATE_PER_SECOND={{ rate_per_second }} QOS={{ qos }} \
      DATASET={{ dataset }} DATASET_PATH={{ dataset_path }} PARTITIONS={{ partitions }} CANDIDATE_MULTIPLIER={{ candidate_multiplier }} \
      SYNOPSIS_BINS={{ synopsis_bins }} \
      scripts/test-all.sh 2>&1 | tee reports/tests/latest.log

# show current Docker services
ps:
    docker compose -f {{ compose_file }} ps

# show Spark master REST/UI summary
spark-ui:
    curl -fsS http://localhost:8080/json/ | python3 -m json.tool

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

# follow Spark submit job logs in Kubernetes
k8s-logs:
    kubectl -n thesis-streaming logs job/spark-topk-submit -f
