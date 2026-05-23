# Paper-Grounded Probabilistic Top-k Stream Task

This project implements a compact Apache Flink DataStream prototype for probabilistic top-k ranking over imperfect stream data. The containerized runtime uses the official `apache/flink:2.2.0-scala_2.12` Docker image.

The implementation follows the thesis/paper interpretation in the prompt:

- incomplete raw records are repaired into probabilistic instances;
- ranking uses query-relative dynamic dominance;
- local lower/upper score bounds prune obvious non-candidates;
- exact refinement is retained for the surviving candidates;
- a pluggable dataset provider produces imperfect stream data for testing and benchmarking.

## Command Runner

Use the `Justfile` as the main CLI entry point:

```bash
just
just test
just bench
just run-local
just e2e
just validate
just monitor-bg
just test-all
```

The recipes wrap the Maven, Docker Compose, curl, monitor, validation, benchmark, and Kubernetes commands used by the project.

## Paper Alignment

The current code is a Flink-based streaming prototype inspired by the supplied PTD and Topk-iDS papers. It validates dynamic dominance, probabilistic instance scoring, candidate pruning/refinement, and an end-to-end MQTT/Kafka/Flink ingestion path. It is not a full reproduction of the target paper's MapReduce/aR-tree distributed PTD algorithm.

The detailed validation report is written to:

```text
reports/validation/paper-alignment.md
```

The algorithm benchmark reports paper-style metrics:

- exact baseline runtime;
- certified pruning runtime and exact top-k agreement;
- fast candidate-pruning runtime;
- precision@k against exact ranking;
- candidate communication reduction;
- partitioned 4-node candidate refinement, shuffle-write proxy bytes, and partitioned precision@k for Spark 3.0 benchmark alignment.

## Run Tests

```bash
just test
```

## Run Benchmark

```bash
OBJECTS=1000 MISSING_RATE=0.25 just bench
```

The benchmark defaults to `DATASET=synthetic`, `PARTITIONS=4`, and `CANDIDATE_MULTIPLIER=4`:

```bash
OBJECTS=1000 QUERIES=2 DIMENSIONS=4 K=10 \
DATASET=synthetic PARTITIONS=4 CANDIDATE_MULTIPLIER=4 \
just bench
```

## Dataset Providers

Datasets are selected through the same CLI surface used by the synthetic generator:

```bash
DATASET=csv DATASET_PATH=/path/to/events.csv just bench
```

Built-in providers:

- `synthetic`: generated incomplete records.
- `csv`: generic normalized CSV records.
- `intel`: `datasets-raw/intel_lab_data.gz`.
- `pump`: `datasets-raw/pump_sensor_data.zip`.
- `gas`: `datasets-raw/gas+sensors+for+home+activity+monitoring.zip`.
- `all`: Intel, pump, and gas together, processed as separate query families.

The CSV provider is intentionally small so new datasets can be adapted later. It expects a header with:

```text
objectId,queryId,eventTime,opType,a0,a1,a2,...
```

`opType` may be blank and defaults to `UPSERT`. Missing values can be blank, `null`, `NaN`, or `?`. Attribute columns can either be named `a0`, `a1`, ... or placed after `opType`. The loader reuses the current default imputation-rule and query-point generators until dataset-specific rules/query files are added.

For Docker Compose or Kubernetes runs, `DATASET_PATH` must be a path visible inside the container, so mount the dataset file or bake it into the image before switching the runtime from `synthetic` to `csv`.

The sender performs dataset preprocessing before publishing. With `DATASET=all`, each raw dataset is published to a separate MQTT topic and bridged to a separate Kafka topic:

```text
thesis/raw/intel -> thesis.raw.intel
thesis/raw/pump  -> thesis.raw.pump
thesis/raw/gas   -> thesis.raw.gas
```

Run a small all-dataset E2E smoke test:

```bash
DATASET=all MAX_EVENTS=5 EXPECTED_MESSAGES=15 just e2e
```

## Run Local Flink Job On The JVM

```bash
just package
OBJECTS=200 QUERIES=2 K=5 just run-local
```

## MQTT -> Kafka -> Flink Stream

The streaming ingress uses EMQX as the MQTT broker. EMQX is configured with a Kafka producer connector, a Kafka sink action, and a rule that forwards MQTT messages from `thesis/raw` to the Kafka topic `thesis.raw.incomplete`. The Flink job can then consume the Kafka topic instead of the in-memory simulator.

Incomplete records are JSON messages shaped like:

```json
{"objectId":"obj-1","queryId":"q0","eventTime":1700000000000,"opType":"UPSERT","attributes":[0.42,null,0.7],"missingMask":[false,true,false]}
```

Run the MQTT publisher against a local broker:

```bash
OBJECTS=200 QUERIES=2 MISSING_RATE=0.35 just publish-local
```

Run the Flink job from Kafka:

```bash
OBJECTS=200 QUERIES=2 K=5 just run-kafka-local
```

Run the same job in the Apache Flink Docker session cluster:

```bash
just image
just setup-fast
OBJECTS=200 QUERIES=2 K=5 just flink-submit
```

The Flink Web UI is exposed at `http://localhost:8081`.

## Kubernetes

Build the local image:

```bash
just image
```

For kind or minikube, make sure the image is available inside the cluster, then apply the manifest:

```bash
just k8s-apply
just k8s-pods
just k8s-logs
```

The Kubernetes flow is:

```text
incomplete-data-publisher -> MQTT topic thesis/raw -> EMQX Kafka sink -> Kafka topic thesis.raw.incomplete -> Apache Flink session cluster
```

## Full E2E Benchmark

Start and configure the local Docker-backed services with scripts and EMQX HTTP API calls:

```bash
just setup
```

This builds `thesis-topk:local` from `apache/flink:2.2.0-scala_2.12`, starts Kafka, EMQX, Flink JobManager, and Flink TaskManager, creates the Kafka topic, then calls `scripts/configure-emqx.sh`, which uses `curl` against the EMQX dashboard API to create:

- Kafka producer connector: `kafka_ingress`
- Kafka producer action: `raw_incomplete_to_kafka`
- MQTT-to-Kafka rule: `mqtt_raw_to_kafka`

Run the full E2E benchmark:

```bash
OBJECTS=200 QUERIES=2 DIMENSIONS=4 K=10 MISSING_RATE=0.35 \
RATE_PER_SECOND=200 QOS=0 WINDOW_MS=10000 \
just e2e
```

The benchmark starts Kafka, EMQX, and the Apache Flink Docker session cluster, publishes incomplete records through MQTT, waits until EMQX has written them to Kafka, then submits Flink in bounded Kafka mode. Results are written to:

```text
reports/e2e/summary.md
reports/e2e/summary.csv
reports/e2e/flink.log
reports/e2e/flink-submit.log
```

Start the realtime monitor GUI:

```bash
PORT=8088 just monitor
```

Open:

```text
http://localhost:8088
```

The monitor polls EMQX, Kafka, and Flink output and shows MQTT ingress, Kafka ingress, Flink outgress, bridge failures/drops, completeness, top-k agreement, prune ratio, and current issues.

Test the monitor metrics from the CLI:

```bash
just monitor-check 400
```

Strictly validate the latest E2E benchmark artifacts and live service state from the CLI:

```bash
just validate 400
```

Run the full verification suite, including unit tests, Docker image build, compose and Kubernetes YAML validation, algorithm performance benchmark, Dockerized E2E benchmark, monitor CLI assertions, and GUI HTTP smoke test:

```bash
OBJECTS=200 QUERIES=2 DIMENSIONS=4 K=10 MISSING_RATE=0.35 \
RATE_PER_SECOND=200 QOS=0 WINDOW_MS=10000 \
just test-all
```
