# Paper-Informed Apache Spark Top-k Dominance Task

This project is upgraded to use **Apache Spark** as the execution layer for probabilistic top-k ranking over uncertain and imperfect data. It follows the 2025 ICCIT PTD direction by keeping the core ideas — probabilistic repair, query-relative dynamic dominance, DSCP-style LB/UB pruning, AES-style compact emission, and exact refinement — while moving the runtime from Hadoop/Flink-style execution to Spark.

## Spark-first Architecture

```text
Raw uncertain events
  -> Spark parallel imputation into probabilistic instances
  -> query/object grouping
  -> distributed LB/UB candidate-bound computation
  -> DSCP-style kth-LB threshold pruning
  -> exact Spark refinement for surviving object groups
  -> TopKResult per query
```

For the full streaming-style demo, the ingress path is also Spark-based:

```text
Python simulator -> EMQX MQTT -> EMQX Kafka sink -> Kafka
  -> Apache Spark bounded Kafka reader
  -> SparkTopKEngine
  -> TopKResult
```

Legacy Flink source files are retained only for comparison. The default build image, Docker Compose services, Kubernetes manifest, E2E scripts, validation scripts, and monitor now target Spark.

## Main Commands

```bash
just
just test
just spark
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
  -Dexec.args="--source=simulator --dataset=synthetic --objects=1000 --queries=2 --dimensions=4 --k=10 --partitions=4 --sparkMaster=local[*]"
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
