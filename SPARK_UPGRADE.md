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
server partition, selects a level to export by estimated index-entry, partial-reference and
reducer-traversal work cost,
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

An independent `docker-compose.hadoop.yml` brings up HDFS and YARN (`namenode`, `datanode`,
`resourcemanager`, `nodemanager`, `historyserver`) and `just hadoop-smoke` validates a real
MapReduce examples job. It is infrastructure evidence only until PTD is implemented as Hadoop
Mapper/Reducer jobs; it must not be compared as if it were the ICCIT algorithm.

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
`algorithmElapsedMs` is the measured paper-shaped query phase total,
`filterMs + emissionMs + refineMs`, summed over queries. `setupMs` keeps input/index/bootstrap
and Spark/JVM scheduling work separate, while `validationMs` keeps oracle work separate.

The aggregate-R-tree execution path broadcasts compact remote index summaries for mapper-side
bound calculation and retains full index leaves only in keyed reducer partitions for
partial-MBR traversal. Heap-ordered local traversal stops excluded candidate subtrees without
creating their partial-emission work. The tree uses Sort-Tile-Recursive spatial packing instead
of single-coordinate stripes, preserving tight MBR bounds for OSM road geometry. Partial records
are materialized and scored only for surviving candidates, in one reducer pass, and serialized
RDDs spill safely with `MEMORY_AND_DISK_SER`.

Research benchmark scripts record `SPARK_MASTER` and default local controlled runs to
`local[4]`; the previous fixed `local[2]` launcher underused the available local CPU. DSCP
combines partition thresholds with a global k-th lower-bound frontier, so it can reject objects
proved unable to enter the global top-k without using exact final scores in filtering.

## Completed Controlled Suites

| Dataset | Suite | Indexed baseline | AES-only | DSCP-only | AES + DSCP |
|---|---|---:|---:|---:|---:|
| Bangladesh OSM road (`98,451` objects) | `iccit-road-full-str-20260527T073041Z` | 18,034 ms | 13,728 ms | 14,493 ms | 12,062 ms |
| Curated smartphone (`20` queries) | `iccit-smartphone-str-20260527T073310Z` | 41,800 ms | 37,050 ms | 43,303 ms | 36,527 ms |

For the OSM run, AES + DSCP is `33.12%` faster than the same-machine indexed Spark baseline
and filters `99.52%` of candidates; the exact smaller OSM fixture
`osm-str-packed-exact-20260527T072842Z` records `exactAgreement=true` and zero false prunes.
For smartphone, AES + DSCP is `12.61%` faster; DSCP-only is slower because it filters only
`3.05%` of objects in exchange for threshold overhead.

These are Spark treatment comparisons. The supplied ICCIT Hadoop figures remain reference
values until PTD Mapper/Reducer jobs are run under the same curated input and controls.
