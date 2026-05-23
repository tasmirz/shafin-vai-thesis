# Apache Spark Upgrade Report

## Scope

This upgrade converts the project from a Flink-first prototype into a Spark-first implementation for the 2025 ICCIT probabilistic top-k dominating-query direction. The original paper uses a Hadoop MapReduce framing for filtering, pruning, shuffle emission, and refinement. The upgraded project keeps the paper-level PTD workflow but implements the distributed execution path with Apache Spark RDDs.

## Spark Components Added

- `com.thesis.topk.spark.SparkTopKEngine`
  - Parallelizes raw uncertain events as Spark RDD partitions.
  - Expands incomplete records into probabilistic instances using the existing DD-style synopsis imputation.
  - Groups instances by query and object.
  - Computes object-level lower and upper candidate bounds.
  - Applies DSCP-style threshold pruning using the kth-largest lower bound.
  - Refines only surviving objects using exact dynamic-dominance scoring.
  - Emits top-k results per query.

- `com.thesis.topk.spark.ProbabilisticTopKSparkJob`
  - Provides a runnable Spark entry point.
  - Supports `--sparkMaster=local[*]` by default and can be pointed to a Spark cluster master.
  - Reports raw event count, probabilistic instance count, pruned/refined object counts, DSCP threshold, compact shuffle-record proxy, and final top-k objects.

## Paper Alignment

| Paper idea | Spark upgrade implementation |
| --- | --- |
| Uncertain objects and probabilistic instances | Raw events are imputed into weighted `ProbabilisticInstance` records. |
| Dynamic dominance relative to a query point | Existing `DominanceScorer.dynamicallyDominates` is reused inside Spark refinement. |
| Lower/upper bound filtering | Spark computes candidate envelopes per object. |
| DSCP | Spark broadcasts the kth-largest lower-bound threshold and filters objects whose upper bound is too weak. |
| AES | Spark shuffles compact object groups instead of repeated instance-competitor records. |
| Exact top-k semantics after pruning | Survivors are refined with exact expected dominance scoring before collecting top-k. |

## How to Run

```bash
just spark
```

or directly:

```bash
mvn -q -DskipTests compile exec:java \
  -Dexec.mainClass=com.thesis.topk.spark.ProbabilisticTopKSparkJob \
  -Dexec.args="--dataset=synthetic --objects=200 --queries=2 --dimensions=4 --k=5 --partitions=4 --sparkMaster=local[*]"
```

## Notes

The older Flink/Kafka files are left in place so previous E2E scripts remain inspectable. The required upgraded path is the Spark entry point above.
