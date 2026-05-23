# Paper Alignment Validation

This report validates the current implementation against the supplied papers:

- `papers/s10115-023-01917-3(Target Paper).pdf`: Distributed probabilistic top-k dominating queries over uncertain databases.
- `papers/10295282.pdf`: Effective and efficient top-k query processing over incomplete data streams.
- `papers/Pre-defense.pdf`: Spark framework proposal combining incomplete-stream imputation and distributed PTD pruning.

## What Matches

| Paper requirement | Current implementation | Status |
| --- | --- | --- |
| Incomplete stream records with missing attributes | `RawEvent`, MQTT publisher, EMQX ingress, Kafka topic | Implemented |
| Imputation into probabilistic instances | `ImputationEngine` emits mutually exclusive `ProbabilisticInstance` records with normalized probabilities | Implemented as simplified DD-style rules |
| Dynamic dominance relative to query point | `DominanceScorer.dynamicallyDominates` implements absolute-distance dominance exactly as the target paper defines it | Implemented |
| PTD object score | `expectedDominanceScore` computes instance probability times dominated probability mass, then sums per object | Implemented for in-memory/global set |
| Top-k agreement against exact ranking | Benchmark validates certified pruned top-k equals exact top-k | Implemented |
| Runtime benchmark | `TopKBenchmark` reports exact, certified-pruned, and fast-candidate runtimes | Implemented |
| Communication/pruning proxy | `candidateCommunicationReduction` estimates reduction from sending/refining only candidate objects | Implemented |
| Precision@k | `fastPrecisionAtK` compares fast candidate pruning against exact top-k | Implemented |
| Dataset plug-in point | `DatasetProvider` supports `synthetic`, `csv`, `intel`, `pump`, `gas`, and combined `all` providers selected by CLI | Implemented |
| Dataset-specific ingress topics | Sender preprocesses Intel/Pump/Gas raw archives and publishes them to separate MQTT/Kafka topic pairs | Implemented |
| 4-node partition benchmark | `TopKBenchmark` partitions objects, emits partition-local candidates, and reports partitioned precision and shuffle-write proxy bytes | Implemented as benchmark model |
| E2E streaming path | MQTT -> EMQX Kafka sink -> Kafka -> Flink 2.2.0 session cluster | Implemented and validated |

## Current Benchmark Results

Command:

```bash
OBJECTS=200 QUERIES=2 DIMENSIONS=4 K=10 MISSING_RATE=0.35 PARTITIONS=4 just bench
```

Latest algorithm benchmark:

```text
summary avgExactMs=48.603 avgCertifiedPrunedMs=109.986 avgFastCandidateMs=6.654 avgCertifiedPruneRatio=0.950 avgFastPruneRatio=0.800 avgCandidateCommunicationReduction=0.800 avgFastPrecisionAtK=1.000 avgPartitionedShuffleWriteBytes=5120 avgPartitionedCommunicationReduction=0.800 avgPartitionedPrecisionAtK=1.000
```

Interpretation:

- Certified pruning preserves exact top-k correctness: `topKAgreement=true` for both queries.
- Fast candidate pruning matches the pre-defense metrics style: runtime, communication reduction, shuffle-write proxy, and precision@k.
- On the current synthetic workload, fast candidate pruning refines 40 of 200 objects per query, giving 80% candidate communication reduction and 1.0 precision@k.
- The partitioned benchmark models a 4-node Spark 3.0 cluster by selecting candidates per partition and reporting candidate shuffle-write bytes. It is a benchmark model, not a Spark runtime execution.

Latest E2E benchmark:

```text
expected_messages: 400
kafka_messages: 400
topk_results: 400
flink_version: 2.2.0
e2e_rate_msg_s: 21.86
```

## Important Gaps

The implementation is a working prototype, not a full reproduction of either paper.

| Gap | Paper expectation | Current state |
| --- | --- | --- |
| Distributed PTD MapReduce phases | Offline partitioning, aR-tree index sharing, filtering mapper, filtering reducer, refinement mapper | Not fully implemented; Flink pipeline is distributed operationally, but algorithm is not the paper's MapReduce/aR-tree design |
| aR-tree score bounds | Lower/upper bounds from dynamic dominance regions and MBR containment/intersection | Not implemented; certified path uses exact-derived bounds, fast path uses candidate proxy |
| Cost model for index level selection | Estimate communication cost and pruning power for index sharing | Not implemented |
| Real datasets | Target paper uses California roads; Topk-iDS paper uses Intel/Pump/Gas | Intel/Pump/Gas archive preprocessors are implemented; dataset-specific DD rules/query calibration still need refinement |
| Topk-iDS PT-k semantics | Probability that an imputed object appears in top-k possible worlds over sliding window | Not implemented; current Flink job emits PTD-style dominance scores over event-time windows |
| Spark requirement from pre-defense | Spark 3.0, shuffle write, 4-node cluster | Benchmark now reports Spark 3.0/4-node/shuffle-write alignment fields; runtime implementation remains Flink 2.2.0 with Kafka, EMQX, Docker Compose, and Kubernetes manifests |

## Validation Decision

The approach is valid as a thesis prototype for a streaming implementation inspired by the papers. It should not be described as a complete reproduction of the target paper's distributed PTD algorithm or the Topk-iDS paper's PT-k sliding-window semantics.

Use this wording:

> The system implements a Flink-based streaming prototype inspired by distributed PTD and Topk-iDS. It validates dynamic dominance, probabilistic instance scoring, pruning/candidate refinement, dataset-provider plug-ins, and an end-to-end MQTT/Kafka/Flink ingestion path. Benchmarking reports exact baseline runtime, candidate-pruned runtime, precision@k, candidate communication reduction, partitioned 4-node shuffle-write proxy, and E2E throughput.

Avoid this wording:

> This fully implements the MapReduce/aR-tree distributed PTD algorithm from Rai and Lian or fully reproduces the Topk-iDS benchmark suite.

## Next Work To Fully Match The Papers

1. Implement aR-tree/MBR summaries per partition.
2. Compute score lower/upper bounds using dynamic dominance regions.
3. Add filtering-mapper, filtering-reducer, and refinement-mapper stages as separate Flink/Spark-like operators.
4. Add real dataset files and dataset-specific adapters for Intel Lab and/or California road network data.
5. Add parameter sweeps for `N`, `k`, dimensionality, object count, missing-rate, and instances per object.
6. Replace shuffle-write proxy bytes with measured Spark 3.0 shuffle write if the final benchmark must run on Spark rather than Flink.
7. Report paper-table metrics: wall-clock time, communication/shuffle MB, pruning power, precision@k/F-score.
