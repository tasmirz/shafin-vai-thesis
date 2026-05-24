# Spark Upgrade Notes

This package upgrades the whole operational path to Spark, not only the Java algorithm package.

## Updated Areas

- Spark RDD engine: `src/main/java/com/thesis/topk/spark/SparkTopKEngine.java`
- Spark application entry point: `src/main/java/com/thesis/topk/spark/ProbabilisticTopKSparkJob.java`
- Bounded Kafka input for Spark jobs using Kafka consumer APIs
- Spark Docker runtime: `Dockerfile` and `Dockerfile.spark`
- Spark Docker Compose stack: `docker-compose.e2e.yml`
- Spark Kubernetes manifest: `k8s/pipeline.yaml`
- Spark E2E script: `scripts/e2e-benchmark.sh`
- Spark service bootstrap: `scripts/setup-services.sh`
- Spark validation: `scripts/validate-e2e.py`
- Spark monitor labels and metrics: `scripts/monitor.py`
- Spark-first recipes: `Justfile`

## Runtime Pipeline

```text
PythonSimulator -> EMQX MQTT -> Kafka -> Spark bounded Kafka reader -> SparkTopKEngine -> TopKResult
```

## Compose Services

- `kafka`
- `emqx`
- `spark-master`
- `spark-worker`
- `spark-submit` profile service for one-off submissions

## Kubernetes Resources

- `spark-master` service and deployment
- `spark-worker` deployment
- `spark-topk-submit` job

## Notes

The legacy Flink Java package remains in the source tree for comparison, but it is no longer the default deployment path.
