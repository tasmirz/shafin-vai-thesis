# Paper Alignment Validation

This report validates the current implementation against the supplied papers:

- `papers/s10115-023-01917-3(Target Paper).pdf`: Distributed probabilistic top-k dominating queries over uncertain databases.
- `papers/10295282.pdf`: Effective and efficient top-k query processing over incomplete data streams.
- `papers/Pre-defense.pdf`: Spark framework proposal combining incomplete-stream imputation and distributed PTD pruning.

## What Matches

| Paper requirement | Current implementation | Status |
| --- | --- | --- |
| Incomplete stream records with missing attributes | `RawEvent`, Python simulator publisher, EMQX ingress, Kafka topic | Implemented |
| Imputation into probabilistic instances | `ImputationEngine` emits mutually exclusive `ProbabilisticInstance` records with normalized probabilities | Implemented as simplified DD-style rules |
| Dynamic dominance relative to query point | `DominanceScorer.dynamicallyDominates` implements absolute-distance dominance exactly as the target paper defines it | Implemented |
| PTD object score | `expectedDominanceScore` computes instance probability times dominated probability mass, then sums per object | Implemented for in-memory/global set |
| Top-k agreement against exact ranking | Benchmark validates certified pruned top-k equals this implementation's exact PTD-style ranking | Implemented as internal consistency, not paper accuracy |
| Runtime benchmark | `TopKBenchmark` reports exact, certified-pruned, and fast-candidate runtimes | Implemented |
| Communication/pruning proxy | `candidateCommunicationReduction` estimates reduction from sending/refining only candidate objects | Implemented |
| Precision@k | `fastPrecisionAtK` compares fast candidate pruning against the same implementation's exact ranking | Implemented as pruning fidelity, not ground-truth F-score |
| Dataset plug-in point | `DatasetProvider` supports `synthetic`, `csv`, `intel`, `pump`, `gas`, and combined `all` providers selected by CLI | Implemented |
| Dataset-specific ingress topics | Decoupled Python sender preprocesses Intel/Pump/Gas raw archives and publishes them to separate MQTT/Kafka topic pairs | Implemented |
| 4-node partition benchmark | `TopKBenchmark` partitions objects, emits partition-local candidates, and reports partitioned precision and shuffle-write proxy bytes | Implemented as benchmark model |
| E2E streaming path | MQTT -> EMQX Kafka sink -> Kafka -> Flink 2.2.0 session cluster | Implemented and validated operationally; outside the papers' measured runtimes |

## Paper Evaluation Requirements

| Source | Paper evaluation basis | Required before numerical comparison |
| --- | --- | --- |
| Rai and Lian, distributed PTD | California road network with 98,451 MBRs and Uniform/Gaussian/Zipf synthetic uncertain objects; objects randomly distributed over `N` servers; 20 random query points; wall-clock time and inter-server communication cost; `N` in `{1,2,5,8,10}`, `k` in `{5,10,15,20,25}`, and `|D|` from `100K` to `1M` | Implement aR-tree summaries, score-bound pruning, MapReduce filtering/refinement, and measured communication over paper-scale inputs |
| Ren, Lian, and Ghazinour, Topk-iDS | Intel (`2.3M` records, four extracted attributes), Pump (`220K`, ten attributes), Gas (`919,438`, ten attributes); known complete records made incomplete by removing `m` attributes; DD imputation plus PT-k over count-based sliding windows; report wall-clock time and F-score against original complete-data top-k | Implement the paper DD rules, count-window PT-k semantics, retained complete ground truth, F-score, and paper parameter sweeps |
| Pre-defense proposal | Apache Spark 3.0 on a four-node cluster with 16 GB RAM per node; Intel and synthetic input; Naive-Centralized versus Distributed-PTD; runtime, measured Shuffle Write MB, and Precision@k | Execute on Spark rather than label a local Java/Flink calculation as Spark; collect Spark metrics and run its baselines |

## Validated Results

### Synthetic Algorithm Check

A repeatable synthetic algorithm check generated `reports/algorithm/topk-100x2.txt` using:

```text
dataset=synthetic objects=100 events=200 dimensions=4 queries=2 k=10 missingRate=0.350 partitions=4
avgExactMs=7.801
avgCertifiedPrunedMs=14.814
avgFastCandidateMs=2.512
avgCertifiedPruneRatio=0.900
avgCandidateCommunicationReduction=0.600
avgFastPrecisionAtK=1.000
avgPartitionedShuffleWriteProxyBytes=5120
avgPartitionedPrecisionAtK=1.000
```

Interpretation:

- `topKAgreement=true` and `avgFastPrecisionAtK=1.000` show agreement against this implementation's exact baseline on two small synthetic queries.
- `avgCandidateCommunicationReduction=0.600` is comparable in direction to a paper pruning/communication objective, but is not measured network traffic.
- `avgPartitionedShuffleWriteProxyBytes=5120` is computed as `candidateCount * 128` in the Java benchmark; it is not Spark Shuffle Write MB.
- This run has 100 objects rather than the target paper's `100K` to `1M` objects, and it does not run the papers' baselines.

### Raw Dataset Routing And Processing Check

The GUI-triggered `raw` E2E test generated the current `reports/e2e/summary.md`:

```text
dataset: all (intel, pump, gas)
topic_messages: thesis.raw.intel=5, thesis.raw.pump=5, thesis.raw.gas=5
expected_messages: 15
kafka_messages: 15
topk_results: 15
flink_version: 2.2.0
e2e_rate_msg_s: 2.70
status: passed
```

The companion algorithm check, `DATASET=all OBJECTS=5 QUERIES=1 DIMENSIONS=4 K=2 just bench`, produced:

```text
dataset provider=all objects=5 events=15 instances=20
intel candidates=1 topKAgreement=true
pump-normal candidates=5 topKAgreement=true
gas candidates=1 topKAgreement=true
avgFastPrecisionAtK=1.000
```

This validates ingestion, preprocessing, topic separation, and execution through Flink. It does **not** validate the Topk-iDS accuracy figures. The current five-row Intel and Gas samples each collapse to one object key, so perfect ranking agreement in those families is trivial and cannot be compared with the paper's Intel/Pump/Gas F-score results.

## Comparability Decision

| Result currently reported | Closest paper measure | Verdict |
| --- | --- | --- |
| Dynamic-dominance/PTD-style score and internal exact agreement | Distributed PTD score semantics in Rai and Lian | Partially aligned at score intent; not a reproduction because aR-tree/MapReduce phases are absent |
| `avgFastPrecisionAtK=1.000` on synthetic input | Topk-iDS F-score; pre-defense Precision@k | Not numerically comparable: it measures pruning agreement with an internal baseline, not accuracy against complete-data ground truth |
| `avgCandidateCommunicationReduction=0.600` | Communication-cost reduction | Directional proxy only; no byte transfer was measured between distributed algorithm stages |
| `avgPartitionedShuffleWriteProxyBytes=5120` | Spark Shuffle Write MB | Not validated against the pre-defense result; calculated proxy, with no Spark executor metrics |
| `2.70 msg/s` E2E for raw routes | No MQTT/Kafka/Flink measure in supplied papers | Valid operational systems result only |
| Raw Intel/Pump/Gas topic processing | Topk-iDS real datasets | Input-family alignment achieved; algorithm and evaluation protocol are not yet matched |

## Important Gaps

The implementation is a working prototype, not a full reproduction of either paper.

| Gap | Paper expectation | Current state |
| --- | --- | --- |
| Distributed PTD MapReduce phases | Offline partitioning, aR-tree index sharing, filtering mapper, filtering reducer, refinement mapper | Not fully implemented; Flink pipeline is distributed operationally, but algorithm is not the paper's MapReduce/aR-tree design |
| aR-tree score bounds | Lower/upper bounds from dynamic dominance regions and MBR containment/intersection | Not implemented; certified path uses exact-derived bounds, fast path uses candidate proxy |
| Cost model for index level selection | Estimate communication cost and pruning power for index sharing | Not implemented |
| Real datasets and ground truth | Target paper uses California roads; Topk-iDS uses complete Intel/Pump/Gas records then removes values to evaluate F-score | Intel/Pump/Gas archive preprocessors are implemented; current short raw smoke samples and generic rules cannot produce comparable F-score |
| Topk-iDS PT-k semantics | Probability that an imputed object appears in top-k possible worlds over sliding window | Not implemented; current Flink job emits PTD-style dominance scores over event-time windows |
| Spark requirement from pre-defense | Spark 3.0, shuffle write, 4-node cluster | Benchmark explicitly identifies its Java-local four-partition model and calculated shuffle proxy; runtime implementation remains Flink 2.2.0 with Kafka, EMQX, Docker Compose, and Kubernetes manifests |

## Validation Decision

The results validate a streaming prototype and ingestion platform inspired by the papers. They do **not** currently match or reproduce any numerical benchmark in the supplied papers. A paper-matching result requires the missing algorithm semantics, ground-truth accuracy protocol, paper-scale workloads, and measured Spark/distributed communication data.

Use this wording:

> The system implements a Flink-based streaming prototype inspired by distributed PTD and Topk-iDS. It validates dynamic dominance, probabilistic instance scoring, pruning/candidate refinement, dataset-provider plug-ins, and an end-to-end MQTT/Kafka/Flink ingestion path. Current benchmark output reports internal ranking agreement and calculated candidate/shuffle proxies; it is not a numerical reproduction of the papers' Spark, MapReduce, or PT-k experiments.

Avoid this wording:

> This fully implements the MapReduce/aR-tree distributed PTD algorithm from Rai and Lian or fully reproduces the Topk-iDS benchmark suite.

## Next Work To Fully Match The Papers

1. Implement aR-tree/MBR summaries per partition.
2. Compute score lower/upper bounds using dynamic dominance regions.
3. Add filtering-mapper, filtering-reducer, and refinement-mapper stages as separate Flink/Spark-like operators.
4. Add California road-network input if matching the distributed PTD target paper is required.
5. Preserve complete Intel/Pump/Gas ground truth, inject missing attributes exactly as Topk-iDS specifies, and compute F-score.
6. Implement the Topk-iDS DD rules, PT-k threshold, and count-based sliding-window parameter sweeps.
7. Add parameter sweeps for `N`, `k`, dimensionality, object count, missing attributes, and instances per object.
8. Replace shuffle-write proxy bytes with measured Spark 3.0 shuffle write if matching the pre-defense benchmark is required.
9. Report paper-table metrics: wall-clock time, measured communication/shuffle MB, pruning power, and ground-truth Precision/Recall/F-score.
