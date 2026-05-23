# Paper-Informed Architecture Validation

This report was originally written for the EMQX/Kafka/Flink prototype. The upgraded repository now adds an Apache Spark-first execution path and validates it against the research motivation and selected query concepts in the supplied papers:

- `papers/s10115-023-01917-3(Target Paper).pdf`: Distributed probabilistic top-k dominating queries over uncertain databases.
- `papers/10295282.pdf`: Effective and efficient top-k query processing over incomplete data streams.
- `papers/An_Efficient_Distributed_Framework_for_Top-k_Dominating_Query_Over_Uncertain_Databases.pdf`: candidate pruning and aggregated distributed emission for PTD.
- `papers/114693.pdf`: continuous uncertain top-k processing under a sliding-window model.
- `papers/3700838.3700859.pdf`: distributed probabilistic skyline processing and pruning context.
- `papers/Space-Efficient_Indexes_for_Uncertain_Strings.pdf`: compact uncertain-data index motivation; its string index is not directly reused.
- `papers/Pre-defense.pdf`: Spark framework proposal combining incomplete-stream imputation and distributed PTD pruning.

## Positioning

This project is **not** a reproduction of the papers' infrastructure or datasets. It now proposes a Spark-first distributed PTD architecture, while retaining the older streaming files for reference:

```text
Python data simulator/preprocessor
  -> EMQX MQTT ingress and Kafka actions
  -> Apache Kafka topic isolation
  -> Apache Spark RDD candidate filtering/refinement
  -> probabilistic top-k result reporting
```

The papers motivate the incomplete-stream problem, probabilistic repair, and top-k/dominance evaluation. The implemented upgrade is to execute an adapted PTD processing path with Apache Spark while preserving the older EMQX/Kafka/Flink pipeline for comparison.

Accordingly:

- California road-network input is not required for this implementation.
- Hadoop MapReduce execution is no longer the target runtime; Spark is the upgraded execution layer.
- aR-tree exchange is not reproduced; Spark object grouping and compact candidate records are used instead.
- Paper metrics are used as conceptual references only where the implemented metric has equivalent meaning.

## Adopted Research Concepts

| Research concept | Current implementation | Validation status |
| --- | --- | --- |
| Incomplete stream records | Python simulator publishes normalized `RawEvent` JSON with `null` attributes and `missingMask` | Implemented and E2E validated |
| Real incomplete-data families | Sender preprocesses Intel, Pump, and Gas archives separately | Implemented and routed separately |
| Probabilistic representation after repair | `DdImputationSynopsis` learns compact conditional histograms and `ImputationEngine` emits normalized probabilistic alternatives | Implemented with cost-selected DD-style synopses |
| Imputation accuracy evidence | Benchmark masks held-out observed values and reports imputation MAE | Implemented; this is not yet paper-style F-score |
| Query-relative dynamic dominance | `DominanceScorer.dynamicallyDominates` evaluates distance from configured query points | Implemented |
| Expected dominance ranking | `expectedDominanceScore` sums probability-weighted dominated mass per object | Implemented |
| Candidate pruning and exact refinement | `ProbabilisticTopK` supports certified and fast candidate paths against its exact ranking baseline | Implemented and internally checked |
| Spark distributed processing objective | Spark groups query/object partitions, computes candidate envelopes, prunes using kth-LB threshold, and refines survivors exactly | Implemented in `com.thesis.topk.spark` |
| Configurable distributed execution | Spark partitions and master are CLI settings through `--partitions` and `--sparkMaster` | Implemented for Spark path |

## Intentional Architectural Replacements

| Source-paper approach | Implemented replacement | Reason and validation target |
| --- | --- | --- |
| Offline/distributed database or MapReduce ingestion | EMQX MQTT ingress with Kafka producer actions | Accept live event feeds and validate ingress completeness and bridge failures |
| Hadoop MapReduce runtime | Apache Spark RDD processing | Use in-memory distributed transformations for candidate filtering and refinement |
| Paper-specific server communication or shuffle reporting | Spark compact object groups and candidate counts | Report compact shuffle-record proxy without claiming byte-equivalence to paper communication cost |
| California road-network experimental source | Intel, Pump, Gas, synthetic, and extensible CSV provider interface | Focus on sensor-stream incomplete data available to this project |
| Single benchmark input flow | Dataset-specific MQTT/Kafka topic isolation | Observe and process each source family independently |

## Implemented Architecture

### Ingress And Routing

For raw dataset execution, the simulator preprocesses each dataset and publishes independently:

| Dataset | MQTT topic | Kafka topic |
| --- | --- | --- |
| Intel | `thesis/raw/intel` | `thesis.raw.intel` |
| Pump | `thesis/raw/pump` | `thesis.raw.pump` |
| Gas | `thesis/raw/gas` | `thesis.raw.gas` |

EMQX is configured either through scripted REST API calls for Docker Compose tests or through the Kubernetes configuration manifest. Its Kafka actions forward MQTT payloads without altering the normalized event schema.

### Spark Processing

The Spark application performs:

```text
Parallelized RawEvent RDD
  -> DD-style synopsis imputation
  -> probabilistic-instance RDD
  -> query/object grouping
  -> DSCP threshold pruning
  -> exact survivor refinement
  -> per-query top-k output
```

The current ranking semantic is PTD-style expected dynamic dominance. It is a deliberate implemented semantic for this Spark upgrade. PT-k possible-world probability can be considered as an additional operator in future work.

## Validated Results

### Synthetic Algorithm Behavior Check

A repeatable algorithm test generated `reports/algorithm/topk-100x2.txt`:

```text
dataset=synthetic objects=100 events=200 instances=662 dimensions=4 queries=2 k=10 missingRate=0.350 partitions=4
executionEngine=java-local partitionModelNodes=4 shuffleMetric=calculated-candidate-proxy
imputationSynopsis rules=8 bins=8 evaluatedValues=104 holdoutMAE=0.169489
avgExactMs=10.457
avgCertifiedPrunedMs=14.812
avgFastCandidateMs=3.044
avgCertifiedPruneRatio=0.855
avgCandidateCommunicationReduction=0.600
avgFastPrecisionAtK=1.000
avgPartitionedShuffleWriteProxyBytes=5120
avgPartitionedPrecisionAtK=1.000
```

Interpretation:

- `avgFastPrecisionAtK=1.000` is agreement with this implementation's exact expected-dominance baseline on the tested synthetic workload.
- `holdoutMAE=0.169489` measures DD-style imputation against values masked from a deterministic synthetic holdout. It is not comparable to a paper F-score until a complete-record missingness protocol is run on real datasets.
- `avgCandidateCommunicationReduction=0.600` and `avgPartitionedShuffleWriteProxyBytes=5120` are candidate-processing indicators, not measured Spark shuffle/network quantities.
- The check validates internal ranking and pruning behavior for the analytics implementation; the upgraded Spark path is available through `just spark`.

### EMQX-Kafka-Flink Raw Dataset E2E Check

The raw pipeline validation after the upgrade produced:

```text
dataset: all (intel, pump, gas)
topic_messages: thesis.raw.intel=5, thesis.raw.pump=5, thesis.raw.gas=5
expected_messages: 15
kafka_messages: 15
topk_results: 15
flink_version: 2.2.0
flink_parallelism: 2
synopsis_bins: 8
e2e_rate_msg_s: 2.04
status: passed
```

Observed bridge and processing status:

| Measure | Result |
| --- | ---: |
| EMQX Kafka actions matched/succeeded | `15 / 15` |
| EMQX Kafka action failures/drops | `0 / 0` |
| Kafka records received | `15 / 15` |
| Flink outputs emitted | `15 / 15` |
| Flink failed jobs | `0` |
| Ingestion completeness | `1.0` |
| Processing completeness | `1.0` |

This validates the replacement architecture path: simulator preprocessing, per-dataset MQTT ingress, EMQX-to-Kafka forwarding, Kafka visibility, and Flink output generation.

## Validation Boundaries

| Area | Valid claim | Claim not made |
| --- | --- | --- |
| Architecture | Spark pipeline executes probabilistic imputation, object grouping, DSCP pruning, and exact refinement | It reproduces every paper-specific Hadoop cluster detail |
| Datasets | Intel, Pump, and Gas are accepted and isolated through ingress topics | California-road experiments are reproduced |
| Ranking | Current PTD-style ranking is internally consistent against its exact baseline on tested data | Current score is identical to Topk-iDS PT-k/F-score semantics |
| Imputation | Compact DD-style conditional histograms select repairs and expose holdout MAE | The full differential-dependency repository/cost model from prior proposals is reproduced |
| Communication | Spark compact candidate records and candidate counts are observable | Proxy records equal exact Spark shuffle bytes or paper communication cost |
| Accuracy | Pipeline completeness and internal pruning fidelity are measured | Paper-style ground-truth F-score has been measured |

## Remaining Work For This Architecture

The remaining tasks below improve the proposed Spark architecture:

| Improvement | Reason |
| --- | --- |
| Preserve complete Intel/Pump/Gas records before simulated missingness and compute Precision/Recall/F-score | Add external accuracy evidence for the chosen ranking/imputation design |
| Calibrate DD-style synopsis rules per dataset and run controlled real-data masking | Turn raw-data MAE/Precision@k into meaningful external accuracy evidence |
| Execute and record Spark cluster scaling measurements beyond `local[*]` | Validate scaling and recovery of the selected Spark execution model |
| Measure Kafka/EMQX throughput over larger raw workloads and failure cases | Validate ingress robustness and realistic throughput |
| Investigate broker-level `messages.dropped` while Kafka action success remains complete | Clarify the operational reliability metric presented in the GUI |
| Optionally add PT-k operator mode | Enable a direct semantic comparison with Topk-iDS if required for analysis, without replacing the architecture |

## Validation Decision

The current result supports the following thesis statement:

> This work upgrades the prior Hadoop/MapReduce-oriented PTD direction into an Apache Spark execution layer for uncertain data. The prototype validates probabilistic repair, Spark object grouping, DSCP-style pruning, AES-style compact candidate records, exact survivor refinement, and top-k result reporting. Paper-derived concepts guide the analytics design; unreproduced paper infrastructure and datasets are not presented as replication requirements.

The following statement should not be used:

> This system reproduces every original MapReduce, aR-tree, California-road, PT-k, or numerical benchmark experiment.
