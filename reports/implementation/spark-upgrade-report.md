# Spark Upgrade Implementation Report

The project has been upgraded so Spark is used across the application, Docker Compose, Kubernetes, scripts, validation, and monitoring layers.

## What Changed

1. `Dockerfile` and `Dockerfile.spark` now build a Spark runtime image from `apache/spark:3.5.3`.
2. `docker-compose.e2e.yml` now starts Kafka, EMQX, Spark master, Spark worker, and a profile-based Spark submit service.
3. `k8s/pipeline.yaml` replaces Flink JobManager/TaskManager resources with Spark master/worker deployments and a `spark-topk-submit` job.
4. `ProbabilisticTopKSparkJob` now supports both generated simulator input and bounded Kafka input.
5. `scripts/setup-services.sh`, `scripts/e2e-benchmark.sh`, `scripts/validate-e2e.py`, `scripts/test-all.sh`, and `scripts/monitor.py` were updated for Spark names, logs, and metrics.
6. `Justfile` recipes now build and submit Spark by default.

## Main Commands

```bash
just spark
just image
just setup
just spark-submit
just e2e
just validate
just k8s-apply
just k8s-logs
```

## Output Files

```text
reports/e2e/spark.log
reports/e2e/spark-submit.log
reports/e2e/summary.md
reports/e2e/summary.csv
```

## Remaining Legacy Code

The `src/main/java/com/thesis/topk/flink` package and older Flink reports are retained for comparison only. The runtime path is Spark-first.
