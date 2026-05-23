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
