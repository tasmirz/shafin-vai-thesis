# Paper-Informed Apache Spark Top-k Dominance Task

This project is upgraded to use **Apache Spark** as the execution layer for probabilistic top-k ranking over uncertain and imperfect data. It follows the 2025 ICCIT PTD direction by keeping the core ideas — probabilistic repair, query-relative dynamic dominance, DSCP-style LB/UB pruning, AES-style compact emission, and exact refinement — while moving the runtime from Hadoop/Flink-style execution to Spark.

## Spark-first Architecture

```text
Raw uncertain events
  -> Spark parallel imputation into probabilistic instances
  -> query/object grouping
  -> distributed LB/UB candidate-bound computation
  -> selectable treatment: baseline | DSCP-only | AES-only | AES + DSCP
  -> optional DSCP-style kth-LB threshold pruning
  -> expanded or AES-aggregated Spark emission/shuffle stage
  -> exact Spark refinement for surviving object groups
  -> TopKResult per query
```

For the finite streaming benchmark, ingress is handled by Spark Structured Streaming:

```text
Python simulator -> EMQX MQTT -> EMQX Kafka sink -> Kafka
  -> Apache Spark Structured Streaming (AvailableNow bounded snapshot)
  -> SparkTopKEngine
  -> TopKResult
```

The bounded snapshot is intentional: each saved PTD benchmark evaluates one fixed set of
uncertain events. It avoids comparing algorithms against different moving stream contents.

Legacy Flink source files are retained only for comparison. The default build image, Docker Compose services, Kubernetes manifest, E2E scripts, validation scripts, and monitor now target Spark.

## Main Commands

```bash
just
just test
just spark
just csv-test
just ablation-test
just stream-test
just compare-runs csv-run-a csv-run-b
just web
just web-test
just web-smoke-test
just image
just setup
just spark-submit
just e2e
just validate
just monitor-bg
just test-all
```

Run Spark locally on generated data:

```bash
OBJECTS=1000 QUERIES=2 DIMENSIONS=4 K=10 PARTITIONS=4 just spark
```

Run the Spark entry point directly:

```bash
mvn -q -DskipTests compile exec:java \
  -Dexec.mainClass=com.thesis.topk.spark.ProbabilisticTopKSparkJob \
  -Dexec.args="--source=simulator --dataset=synthetic --objects=1000 --queries=2 --dimensions=4 --k=10 --partitions=4 --algorithm=aes-dscp --sparkMaster=local[*]"
```

## Docker Compose

Build the Spark runtime image:

```bash
just image
```

Start Kafka, EMQX, Spark master, and Spark worker:

```bash
just setup
```

Spark services exposed by `docker-compose.e2e.yml`:

- Spark master RPC: `spark://localhost:7077`
- Spark master UI: `http://localhost:8080`
- Spark worker UI: `http://localhost:8081`
- Kafka external listener: `localhost:29092`
- EMQX MQTT: `localhost:18884`
- EMQX dashboard/API: `http://localhost:18084`

Submit the bounded Kafka Spark job to the Compose Spark cluster:

```bash
EXPECTED_MESSAGES=400 OBJECTS=200 QUERIES=2 K=10 just spark-submit
```

Run the full Dockerized E2E benchmark:

```bash
OBJECTS=200 QUERIES=2 DIMENSIONS=4 K=10 MISSING_RATE=0.35 \
RATE_PER_SECOND=200 QOS=0 PARTITIONS=4 just e2e
```

E2E artifacts:

```text
reports/e2e/summary.md
reports/e2e/summary.csv
reports/e2e/spark.log
reports/e2e/spark-submit.log
```

The E2E profile now uses Spark's Kafka Structured Streaming source rather than a driver-side
Kafka consumer. It requires the `spark-sql-kafka-0-10_2.12` connector packaged with the app.

## Kubernetes

Build the image and make it available to your cluster, then apply the manifest:

```bash
just image
just simulator-image
just k8s-apply
just k8s-pods
just k8s-logs
```

The Kubernetes manifest creates:

- `Namespace/thesis-streaming`
- `StatefulSet/kafka`
- `Deployment/emqx`
- `Deployment/incomplete-data-publisher`
- `Deployment/spark-master`
- `Deployment/spark-worker`
- `Job/spark-topk-submit`

The Spark submit job consumes Kafka topics and submits to `spark://spark-master:7077`.

## Dataset Providers

Datasets are selected through the same CLI surface used by the synthetic generator:

```bash
DATASET=csv DATASET_PATH=/path/to/events.csv just spark
```

Built-in providers:

- `synthetic`: generated incomplete records.
- `csv`: generic normalized CSV records.
- `intel`: `datasets-raw/intel_lab_data.gz`.
- `pump`: `datasets-raw/pump_sensor_data.zip`.
- `gas`: `datasets-raw/gas+sensors+for+home+activity+monitoring.zip`.
- `all`: Intel, pump, and gas together, processed as separate query families.

CSV header format:

```text
objectId,queryId,eventTime,opType,a0,a1,a2,...
```

Missing values can be blank, `null`, `NaN`, or `?`. `opType` may be blank and defaults to `UPSERT`.

For Docker/Kubernetes runs, `DATASET_PATH` must be visible inside the container. The Spark image looks under `/opt/spark/datasets-raw` for baked-in raw datasets.

## Research Test Profiles And Saved Runs

Run the deterministic CSV fixture through Spark and exactness validation:

```bash
RUN_ID=csv-baseline just csv-test
```

Run the controlled four-treatment ablation on one deterministic input setup:

```bash
RUN_ID=paper-ablation BUILD_IMAGE=0 just ablation-test
```

Selectable algorithm IDs are `baseline`, `dscp-only`, `aes-only`, and `aes-dscp`.
Each saved query records the executed emitted-record count, baseline/AES emission counts,
`AER`, pruning ratio, and the exact-oracle `falsePrunes` audit.

The fixture is `tests/fixtures/csv/smartphone-small.csv`. To exercise another normalized CSV:

```bash
RUN_ID=csv-custom CSV_PATH=/absolute/path/events.csv scripts/research/run_csv_benchmark.sh
```

Run the actual stream route with a finite reproducible workload:

```bash
RUN_ID=stream-baseline OBJECTS=12 QUERIES=2 DIMENSIONS=2 K=2 just stream-test
```

To keep a command-line validation run from updating the latest shared E2E
snapshot files, direct its transient logs to another directory:

```bash
RUN_ID=stream-check E2E_REPORT_DIR=/tmp/ptd-stream-check \
  BUILD_IMAGE=0 tests/e2e/test_mqtt_kafka_spark.sh
python3 scripts/validate-e2e.py --report-dir /tmp/ptd-stream-check \
  --expected-messages 24 --expected-queries 2
```

`just test-all` uses isolated temporary output automatically while still
persisting immutable completed-run evidence under `reports/runs/<run-id>/`.

## PTD-BenchLab Website

Start the local research workbench:

```bash
just web
```

Open `http://127.0.0.1:8090`. Set another port with `WEB_PORT=8091 just web`.

The dependency-free website is backed directly by the saved experiment evidence in
`reports/runs/*`. It provides:

- a dashboard for validated CSV and MQTT/Kafka/Spark runs;
- click-through run details with configuration, per-query pruning, thresholds and logs;
- fair-comparison warnings when seeds, partitions, dataset hashes or other controls differ;
- CSV record inspection and input-quality summaries;
- launch controls for the validated CSV and bounded streaming benchmark profiles;
- exactness status, measured runtime/pruning charts, and downloadable evidence bundles.

The site can launch all four treatment variants and compare their saved AER/pruning evidence.
Spatial road simulation, per-object DDR/MBR traces and actual Spark shuffle-byte metrics remain
pending instrumentation; it does not present those claims from placeholder data.

Validate both its artifact logic and its served HTTP endpoints from the terminal:

```bash
just web-test
just web-smoke-test
```

Every profile produces an immutable run directory under `reports/runs/<run-id>/`:

```text
manifest.json       configuration, dataset SHA-256, commit and dirty-worktree state
metrics.json        Spark/PTD metrics, algorithm time, validation time, correctness status
metrics.csv         query-level variant, emission, AER, pruning and validation export
spark.log           execution evidence
algorithm.log       CSV profile exact-vs-pruned validation evidence
e2e-summary.csv     stream profile ingress and elapsed-time evidence
```

Compare two saved setups:

```bash
just compare-runs csv-baseline csv-partitions-4
```

The comparison uses algorithm time excluding the optional oracle-check duration, and warns if
fairness-critical setup fields differ, including dataset checksum, data shape, `k`, partition
count, or seed.

## MQTT/Kafka Ingress

The simulator publishes normalized JSON records such as:

```json
{"objectId":"obj-1","queryId":"q0","eventTime":1700000000000,"opType":"UPSERT","attributes":[0.42,null,0.7],"missingMask":[false,true,false]}
```

With `DATASET=all`, each raw dataset is published to a separate MQTT topic and bridged to a separate Kafka topic:

```text
thesis/raw/intel -> thesis.raw.intel
thesis/raw/pump  -> thesis.raw.pump
thesis/raw/gas   -> thesis.raw.gas
```

Run a small all-dataset Spark E2E smoke test:

```bash
DATASET=all MAX_EVENTS=5 EXPECTED_MESSAGES=15 QUERIES=1 K=2 just e2e
```

## Monitoring and Validation

Start the monitor:

```bash
PORT=8088 just monitor
```

The monitor reads Spark logs and E2E summary files, then reports MQTT, Kafka, Spark TopKResult count, ingestion completeness, pruning metrics, and imputation MAE.

Validate the latest run:

```bash
EXPECTED_MESSAGES=400 QUERIES=2 just validate
```

Run full checks:

```bash
just test-all
```

## Reports

Spark upgrade report:

```text
reports/implementation/spark-upgrade-report.md
```

Paper alignment report:

```text
reports/validation/paper-alignment.md
```
