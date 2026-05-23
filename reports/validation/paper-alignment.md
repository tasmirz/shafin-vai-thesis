# Paper-Informed Architecture Validation

This report validates the implemented EMQX, Kafka, and Apache Flink architecture against the research motivation and selected query concepts in the supplied papers:

- `papers/s10115-023-01917-3(Target Paper).pdf`: Distributed probabilistic top-k dominating queries over uncertain databases.
- `papers/10295282.pdf`: Effective and efficient top-k query processing over incomplete data streams.
- `papers/Pre-defense.pdf`: Spark framework proposal combining incomplete-stream imputation and distributed PTD pruning.

## Positioning

This project is **not** a reproduction of the papers' infrastructure or datasets. It proposes a replacement streaming architecture:

```text
Python data simulator/preprocessor
  -> EMQX MQTT ingress and Kafka actions
  -> Apache Kafka topic isolation
  -> Apache Flink event-time processing
  -> probabilistic top-k result monitoring
```

The papers motivate the incomplete-stream problem, probabilistic repair, and top-k/dominance evaluation. The implemented contribution is to execute an adapted processing path as an observable realtime event pipeline using EMQX, Kafka, Flink, Docker Compose, and Kubernetes.

Accordingly:

- California road-network input is not required for this implementation.
- Spark 3.0 and MapReduce execution are not target runtime requirements.
- aR-tree exchange and Spark shuffle metrics are not missing architecture components; they belong to approaches that this system replaces.
- Paper metrics are used as conceptual references only where the implemented metric has equivalent meaning.

## Adopted Research Concepts

| Research concept | Current implementation | Validation status |
| --- | --- | --- |
| Incomplete stream records | Python simulator publishes normalized `RawEvent` JSON with `null` attributes and `missingMask` | Implemented and E2E validated |
| Real incomplete-data families | Sender preprocesses Intel, Pump, and Gas archives separately | Implemented and routed separately |
| Probabilistic representation after repair | `ImputationEngine` emits mutually exclusive `ProbabilisticInstance` alternatives with normalized probabilities | Implemented using simplified rules |
| Query-relative dynamic dominance | `DominanceScorer.dynamicallyDominates` evaluates distance from configured query points | Implemented |
| Expected dominance ranking | `expectedDominanceScore` sums probability-weighted dominated mass per object | Implemented |
| Candidate pruning and exact refinement | `ProbabilisticTopK` supports certified and fast candidate paths against its exact ranking baseline | Implemented and internally checked |
| Continuous/stream processing objective | Flink consumes Kafka streams and emits event-time-window top-k results | Implemented |

## Intentional Architectural Replacements

| Source-paper approach | Implemented replacement | Reason and validation target |
| --- | --- | --- |
| Offline/distributed database or MapReduce ingestion | EMQX MQTT ingress with Kafka producer actions | Accept live event feeds and validate ingress completeness and bridge failures |
| Spark/MapReduce runtime | Apache Flink `2.2.0` DataStream processing | Execute stateful event-time streaming and validate job completion/output |
| Paper-specific server communication or shuffle reporting | Kafka topic offsets, EMQX action metrics, E2E timing, and calculated candidate proxy | Measure the proposed architecture honestly without presenting proxies as Spark communication metrics |
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

### Flink Processing

The Flink application performs:

```text
KafkaSource
  -> RawEvent deserialization
  -> event-time watermark assignment
  -> probabilistic imputation
  -> query-family keyed state
  -> time-window top-k ranking
  -> TopKResult output
```

The current ranking semantic is PTD-style expected dynamic dominance. It is a deliberate implemented semantic for this architecture. PT-k possible-world probability can be considered as an additional operator in future work, but its absence does not make the EMQX/Kafka/Flink architecture invalid.

## Validated Results

### Synthetic Algorithm Behavior Check

A repeatable algorithm test generated `reports/algorithm/topk-100x2.txt`:

```text
dataset=synthetic objects=100 events=200 dimensions=4 queries=2 k=10 missingRate=0.350 partitions=4
executionEngine=java-local partitionModelNodes=4 shuffleMetric=calculated-candidate-proxy
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

- `avgFastPrecisionAtK=1.000` is agreement with this implementation's exact expected-dominance baseline on the tested synthetic workload.
- `avgCandidateCommunicationReduction=0.600` and `avgPartitionedShuffleWriteProxyBytes=5120` are candidate-processing indicators, not measured Spark shuffle/network quantities.
- The check validates internal ranking and pruning behavior for the Flink-oriented implementation.

### EMQX-Kafka-Flink Raw Dataset E2E Check

The GUI-triggered raw pipeline validation produced:

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
| Architecture | EMQX/Kafka/Flink pipeline executes raw incomplete events end to end | It reproduces the papers' MapReduce or Spark runtime |
| Datasets | Intel, Pump, and Gas are accepted and isolated through ingress topics | California-road experiments are reproduced |
| Ranking | Current PTD-style ranking is internally consistent against its exact baseline on tested data | Current score is identical to Topk-iDS PT-k/F-score semantics |
| Communication | Kafka/EMQX traffic and calculated candidate proxies are observable | Proxy bytes equal Spark Shuffle Write or paper communication cost |
| Accuracy | Pipeline completeness and internal pruning fidelity are measured | Paper-style ground-truth F-score has been measured |

## Remaining Work For This Architecture

The remaining tasks below improve the proposed Flink architecture rather than reproduce an excluded implementation:

| Improvement | Reason |
| --- | --- |
| Preserve complete Intel/Pump/Gas records before simulated missingness and compute Precision/Recall/F-score | Add external accuracy evidence for the chosen ranking/imputation design |
| Implement dataset-specific imputation rules or configurable rule loading | Replace generic repair assumptions with dataset-informed behavior |
| Make Flink parallelism configurable and run parallel Kubernetes E2E measurements | Validate scaling of the selected Flink execution model |
| Measure Kafka/EMQX throughput over larger raw workloads and failure cases | Validate ingress robustness and realistic throughput |
| Investigate broker-level `messages.dropped` while Kafka action success remains complete | Clarify the operational reliability metric presented in the GUI |
| Optionally add PT-k operator mode | Enable a direct semantic comparison with Topk-iDS if required for analysis, without replacing the architecture |

## Validation Decision

The current result supports the following thesis statement:

> This work replaces prior offline/Spark-oriented execution assumptions with an event-driven incomplete-data processing architecture built on EMQX MQTT ingress, Apache Kafka topic routing, and Apache Flink event-time analytics. The prototype validates per-dataset ingestion for Intel, Pump, and Gas streams, probabilistic repair and PTD-style top-k processing, realtime observability, and end-to-end execution completeness. Paper-derived concepts guide the analytics design; excluded paper infrastructure and datasets are not presented as replication requirements.

The following statement should not be used:

> This system reproduces the original papers' MapReduce, Spark, aR-tree, California-road, PT-k, or numerical benchmark experiments.
