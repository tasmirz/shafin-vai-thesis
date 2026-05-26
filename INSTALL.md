# Install and Run

## Requirements

- Java 17
- Maven
- Docker with Docker Compose v2
- Python 3.10+
- `just`
- Optional for Kubernetes: `kubectl`, kind/minikube, and a way to load local images

## Build

```bash
just test
just image
```

The default Docker image is now Spark-based:

```text
thesis-topk-spark:local
```

It contains:

```text
/opt/spark/app/topk-spark.jar
/opt/spark/datasets-raw
```

## Local Spark Run

```bash
OBJECTS=200 QUERIES=2 DIMENSIONS=4 K=10 PARTITIONS=4 just spark
```

## Docker Compose Spark Stack

```bash
just setup
```

This starts:

```text
Kafka + EMQX + Spark master + Spark worker
```

Submit the Spark Kafka job:

```bash
EXPECTED_MESSAGES=400 OBJECTS=200 QUERIES=2 just spark-submit
```

Run complete E2E:

```bash
OBJECTS=200 QUERIES=2 EXPECTED_MESSAGES=400 just e2e
```

## Research Validation Profiles

The CSV profile is deterministic and runs inside the Java 17 Spark image, independent of the
host Java version:

```bash
RUN_ID=csv-baseline just csv-test
```

Run all controlled algorithm treatments over the same CSV fixture:

```bash
RUN_ID=csv-ablation BUILD_IMAGE=0 just ablation-test
```

For one selected treatment, set `ALGORITHM=baseline`, `ALGORITHM=dscp-only`,
`ALGORITHM=aes-only`, or `ALGORITHM=aes-dscp`.

The streaming profile retains the complete MQTT -> Kafka -> Spark route and uses Spark
Structured Streaming to drain the finite test snapshot:

```bash
RUN_ID=stream-baseline OBJECTS=12 QUERIES=2 K=2 just stream-test
```

Compare preserved run artifacts:

```bash
just compare-runs csv-baseline stream-baseline
```

Automated full validation isolates transient E2E output from tracked result snapshots:

```bash
OBJECTS=12 QUERIES=2 DIMENSIONS=2 K=2 PARTITIONS=2 just test-all
```

## PTD-BenchLab Website

Serve the local research workbench over saved run artifacts:

```bash
just web
```

Open `http://127.0.0.1:8090`. The site can launch the validated CSV and
MQTT/Kafka/Spark profiles, inspect CSV records, compare saved runs with
fairness warnings, inspect validation/log evidence, and export bundles.

Validate the website backend:

```bash
just web-test
just web-smoke-test
```

## Kubernetes

```bash
just image
just simulator-image
just k8s-apply
just k8s-pods
just k8s-logs
```

The manifest uses Spark resources named `spark-master`, `spark-worker`, and `spark-topk-submit`.

## Cleanup

```bash
just clean
```
