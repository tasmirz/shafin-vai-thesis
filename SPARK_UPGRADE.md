# Spark Upgrade Notes

This package upgrades the whole operational path to Spark, not only the Java algorithm package.

## Updated Areas

- Spark RDD engine: `src/main/java/com/thesis/topk/spark/SparkTopKEngine.java`
- Spark application entry point: `src/main/java/com/thesis/topk/spark/ProbabilisticTopKSparkJob.java`
- Bounded Kafka input through Spark Structured Streaming and `Trigger.AvailableNow()`
- Spark Docker runtime: `Dockerfile` and `Dockerfile.spark`
- Spark Docker Compose stack: `docker-compose.e2e.yml`
- Spark Kubernetes manifest: `k8s/pipeline.yaml`
- Spark E2E script: `scripts/e2e-benchmark.sh`
- Spark service bootstrap: `scripts/setup-services.sh`
- Spark validation: `scripts/validate-e2e.py`
- Spark monitor labels and metrics: `scripts/monitor.py`
- Spark-first recipes: `Justfile`
- Research profiles and fixtures: `tests/fixtures`, `tests/integration`, `tests/e2e`
- Saved run/comparison tooling: `scripts/research` and `reports/runs`
- Rai-Lian aggregate R-tree and partial-node traversal: `src/main/java/com/thesis/topk/algorithm/index/AggregateRTree.java`

## Runtime Pipeline

```text
PythonSimulator -> EMQX MQTT -> Kafka -> Spark Structured Streaming bounded reader -> SparkTopKEngine -> TopKResult
```

The AvailableNow query processes a finite Kafka snapshot after MQTT publication and terminates.
This is the reproducible streaming-ingress test surface; PTD ranking remains a fixed-dataset
evaluation so repeated setup comparisons have a stable input population.

For curated MBR datasets, `SparkTopKEngine` constructs an aggregate R-tree for each logical
server partition, selects a level to export by estimated index-entry plus partial-reference cost,
and sends only partial MBR references through a reducer-shaped Spark stage. Fully dominated
entries contribute aggregate mass directly; partial entries are recursively traversed to object
instances before exact top-k output. This path is identified in saved runs as
`rai-lian-artree-selected-level-partial-reducer`.

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

## Research Profiles

```bash
RUN_ID=csv-baseline just csv-test
RUN_ID=stream-baseline just stream-test
just compare-runs csv-baseline csv-alternative
```

Saved run directories include a manifest, metrics exports, logs, dataset checksum when a source
file is used, and exact-agreement evidence where the oracle benchmark is run. Reported
`algorithmElapsedMs` excludes the measured oracle validation duration.
