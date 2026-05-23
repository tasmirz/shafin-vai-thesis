# EMQX-Kafka-Flink Incomplete Stream Processing Implementation Report

**Project:** Paper-Grounded Probabilistic Top-k Stream Task  
**Report date:** 2026-05-23  
**Implementation status:** Executable prototype with Docker Compose E2E verification and Kubernetes manifests  

## Executive Summary

The current system is an event-streaming prototype for processing incomplete sensor records through an MQTT-to-Kafka ingress path and an Apache Flink ranking pipeline. A decoupled Python simulator preprocesses synthetic or raw Intel, Pump, and Gas dataset records into a normalized incomplete-event JSON schema and publishes them over MQTT. EMQX accepts the messages and uses Kafka producer actions to route each dataset stream into Kafka. Apache Flink consumes the Kafka records, imputes missing attributes into probabilistic instances, computes probabilistic dynamic-dominance top-k results in event-time windows, and emits result records.

The implementation currently provides:

- a Python data sender isolated in its own virtual environment and container image;
- separate MQTT and Kafka topic routes for the three raw datasets;
- scripted EMQX Kafka connector, action, and rule configuration through HTTP API calls;
- Docker Compose execution for repeatable end-to-end benchmarking;
- Kubernetes resources for the same core services and an application-mode Flink deployment;
- a realtime monitoring GUI with traffic, completeness, issue, benchmark, and CLI test-run visibility;
- automated CLI verification through a `Justfile`.

The latest validated raw-route execution processed 15 records end to end:

| Measure | Observed value |
| --- | ---: |
| MQTT messages published by simulator | 15 |
| EMQX Kafka action matches/successes | 15 / 15 |
| EMQX Kafka action failures/drops | 0 / 0 |
| Kafka records | 15 |
| Flink `TopKResult` outputs | 15 |
| Ingestion completeness | 100% |
| Processing completeness | 100% |
| Total E2E time | 5,563 ms |
| E2E throughput | 2.70 messages/s |

This result validates the implemented systems path. It does not constitute numerical reproduction of the supplied research papers' Spark, MapReduce/aR-tree, or PT-k accuracy experiments.

## 1. System Purpose And Scope

The implementation addresses the operational problem of ingesting incomplete data streams from multiple dataset families and processing them in a streaming analytics engine. Its current scope is:

1. Prepare incomplete or missing-valued events at the sender.
2. Transport events through MQTT ingress.
3. Bridge MQTT data into durable Kafka topics.
4. Consume events in Flink.
5. Expand missing fields into probabilistic alternatives.
6. Produce top-k ranked results using query-relative dynamic dominance.
7. Measure message flow, service state, and E2E timing.

The system supports both generated records and raw dataset archive readers. The raw datasets are kept on distinct topic routes so each dataset remains observable and processable as its own query family.

## 2. Architecture

### 2.1 End-To-End Flow

```text
                +------------------------+
                | Python Simulator       |
                | preprocess + normalize |
                +-----------+------------+
                            |
                            | MQTT JSON records
                            v
                +-----------+------------+
                | EMQX MQTT Broker       |
                | rule engine + actions  |
                +-----------+------------+
                            |
                            | Kafka producer connector
                            v
                +-----------+------------+
                | Apache Kafka           |
                | per-dataset topics     |
                +-----------+------------+
                            |
                            | Flink KafkaSource
                            v
                +-----------+------------+
                | Apache Flink 2.2.0     |
                | imputation + top-k     |
                +-----------+------------+
                            |
                            | TopKResult output/log
                            v
                +-----------+------------+
                | Monitor + Validation   |
                | GUI and CLI reports    |
                +------------------------+
```

### 2.2 Primary Components

| Component | Implementation | Responsibility |
| --- | --- | --- |
| Sender/simulator | Python 3, `paho-mqtt==2.1.0` | Reads datasets, normalizes missing data, publishes MQTT messages |
| MQTT ingress and bridge | EMQX Enterprise `5.8.4` | Accepts MQTT messages and routes them through Kafka producer actions |
| Message bus | Apache Kafka `3.8.1` | Stores ingress records by topic for Flink consumption |
| Processing engine | Apache Flink `2.2.0-scala_2.12` | Reads Kafka data, imputes missing values, computes top-k outputs |
| E2E orchestration | Bash scripts and `Justfile` | Builds, configures, executes, validates, and reports the pipeline |
| Realtime monitor | Python HTTP GUI/API | Displays live counts, status, benchmark indicators, and CLI test output |

## 3. Data Sender And Event Contract

### 3.1 Decoupled Simulator

The sender is implemented independently from the Java/Flink application in `scripts/simulator.py`. This decoupling keeps raw file decoding and MQTT publication outside the streaming job, and allows the same sender to be exercised through a local Python virtual environment or a dedicated Kubernetes image.

Supported input providers:

| Provider | Source | Published query family |
| --- | --- | --- |
| `synthetic` | Generated incomplete numeric events | `q0`, `q1`, ... |
| `csv` | User-provided normalized CSV | Value from CSV |
| `intel` | `datasets-raw/intel_lab_data.gz` | `intel` |
| `pump` | `datasets-raw/pump_sensor_data.zip` | `pump-<machine_status>` |
| `gas` | `datasets-raw/gas+sensors+for+home+activity+monitoring.zip` | `gas` |
| `all` | Intel, Pump, and Gas in one execution | Separate families and topics |

The simulator represents absent, `null`, `NaN`, `?`, or blank attribute values as missing values and supplies a Boolean mask so downstream deserialization does not infer missingness from numeric sentinel values.

### 3.2 Normalized Record Schema

Every sender output is a JSON object with this logical contract:

```json
{
  "objectId": "pump-row-0",
  "queryId": "pump-normal",
  "eventTime": 1533081600000,
  "opType": "UPSERT",
  "attributes": [0.42, null, 0.70],
  "missingMask": [false, true, false]
}
```

| Field | Meaning |
| --- | --- |
| `objectId` | Identity used to group probabilistic alternatives for ranking |
| `queryId` | Query family used to key Flink state and select a query point |
| `eventTime` | Event timestamp used for Flink event-time processing |
| `opType` | Operation marker; currently published as `UPSERT` |
| `attributes` | Numeric input dimensions; missing attributes are JSON `null` |
| `missingMask` | Position-aligned indicator of missing attributes |

### 3.3 Dataset Topic Isolation

For `DATASET=all`, the sender and EMQX bridge use separate routes:

| Dataset | MQTT topic | Kafka topic |
| --- | --- | --- |
| Intel | `thesis/raw/intel` | `thesis.raw.intel` |
| Pump | `thesis/raw/pump` | `thesis.raw.pump` |
| Gas | `thesis/raw/gas` | `thesis.raw.gas` |

For synthetic/default tests, a single generic route is used:

```text
thesis/raw -> thesis.raw.incomplete
```

## 4. MQTT Ingress And Kafka Bridge

### 4.1 EMQX Configuration Path

In Docker Compose E2E execution, `scripts/configure-emqx.sh` performs EMQX setup through the dashboard REST API:

1. Authenticate to EMQX.
2. Remove any previous connector configuration for a clean run.
3. Create Kafka producer connector `kafka_ingress`.
4. For each configured topic mapping, create a Kafka producer action.
5. For each action, create an MQTT rule selecting payload and metadata from the source MQTT topic.

The script accepts `TOPIC_MAPPINGS` as comma-separated `mqtt=kafka` pairs, enabling new dataset routes without changing sender or bridge implementation.

For the current raw E2E run, the created Compose-side actions are:

| EMQX action | Kafka target |
| --- | --- |
| `raw_incomplete_to_kafka_thesis_raw_intel` | `thesis.raw.intel` |
| `raw_incomplete_to_kafka_thesis_raw_pump` | `thesis.raw.pump` |
| `raw_incomplete_to_kafka_thesis_raw_gas` | `thesis.raw.gas` |

The action parameters use:

| Parameter | Configuration |
| --- | --- |
| Kafka connector | `kafka_ingress` |
| Message key | MQTT `clientid` |
| Message value | MQTT `payload` |
| Required acknowledgements | `all_isr` |
| Query mode | `async` |
| Partition strategy | `random` |

### 4.2 Latest Bridge Evidence

The current monitoring API reports the following EMQX Kafka action totals for the raw E2E execution:

| Metric | Intel | Pump | Gas | Total |
| --- | ---: | ---: | ---: | ---: |
| Matched | 5 | 5 | 5 | 15 |
| Successful | 5 | 5 | 5 | 15 |
| Failed | 0 | 0 | 0 | 0 |
| Dropped by Kafka action | 0 | 0 | 0 | 0 |

EMQX global broker metrics also report `messages.dropped=15`. This is distinct from the Kafka action metrics and does not indicate failed Kafka forwarding in this run: all action deliveries succeeded and all 15 records are present in Kafka. The global dropped-delivery count should remain visible as an operational diagnostic and should be investigated before using MQTT broker-level drop rate as a reliability KPI.

## 5. Kafka Layer

Kafka is deployed in single-node KRaft mode for the prototype.

### 5.1 Docker Compose Configuration

| Item | Configuration |
| --- | --- |
| Container image | `apache/kafka:3.8.1` |
| Internal listener | `kafka:9092` |
| Host-facing listener | `localhost:29092` |
| Controller listener | `9093` |
| Roles | `controller,broker` |
| Topic auto-creation | Enabled |
| Persistent volume | `kafka-data` |

### 5.2 Consumption Behavior

The Flink job selects Kafka topics from the chosen dataset provider:

- a specific provider reads its mapped Kafka topic;
- provider `all` consumes the Intel, Pump, and Gas topics together.

For benchmark executions, Flink uses an earliest start offset and a bounded latest end offset, allowing the job to drain the known test records and finish deterministically.

## 6. Flink Processing Pipeline

### 6.1 Runtime Deployment

The processing image is built from:

```dockerfile
FROM apache/flink:2.2.0-scala_2.12
```

It includes the shaded Java application JAR and raw datasets under `/opt/flink`. In local Docker Compose execution, a Flink session cluster is started and the bounded job is submitted to its REST endpoint. The latest verified Docker deployment has:

| Runtime item | Observed value |
| --- | ---: |
| Flink version | `2.2.0` |
| JobManagers | 1 |
| TaskManagers | 1 |
| Total task slots | 2 |
| Finished jobs | 1 |
| Failed jobs | 0 |

### 6.2 Application Dataflow

`ProbabilisticTopKJob` implements:

```text
KafkaSource<String>
  -> RawEventJsonSerde.fromJson
  -> event-time timestamps and bounded-out-of-orderness watermarks
  -> FlinkImputationFunction
  -> keyBy(queryId)
  -> TopKProcessFunction
  -> TopKResult output
```

Relevant settings:

| Setting | Current behavior |
| --- | --- |
| Job parallelism | Set in code to `1` |
| Default event-time window | `30,000 ms`; E2E test uses `10,000 ms` |
| Watermark out-of-orderness | Default `1,000 ms` |
| Watermark interval | `100 ms` |
| Kafka bounded test mode | Enabled during E2E validation |

### 6.3 Missing-Value Processing

The imputation function converts each incomplete raw event into one or more `ProbabilisticInstance` alternatives. For each missing dimension:

1. Retrieve an imputation rule for that dimension or use a default rule.
2. Generate low-value and high-value alternatives.
3. Multiply alternative probabilities as multiple missing fields are expanded.
4. Normalize probabilities for the event after expansion.

This implementation is a simplified rule-based probabilistic expansion. It is not yet the paper's full DD repository lookup or cost-model-driven imputation.

### 6.4 Ranking Processing

For each keyed query family, `TopKProcessFunction` keeps live probabilistic instances in Flink state, removes expired instances when event-time timers fire, and executes pruned top-k ranking over the remaining window.

The ranking implementation:

- evaluates dynamic dominance relative to a configured query point;
- calculates expected dominance score using instance probabilities and dominated probability mass;
- computes lower and upper scores for pruning candidates;
- exactly refines surviving candidates;
- emits `TopKResult`.

This is a PTD-style ranking prototype. It is not currently PT-k possible-world probability evaluation over count-based windows.

## 7. Deployment Modes

### 7.1 Docker Compose E2E Deployment

`docker-compose.e2e.yml` runs:

| Service | Image | Role |
| --- | --- | --- |
| `kafka` | `apache/kafka:3.8.1` | Local message bus |
| `emqx` | `emqx/emqx-enterprise:5.8.4` | MQTT broker and Kafka bridge |
| `flink-jobmanager` | `thesis-topk:local` | Flink session cluster coordinator and UI |
| `flink-taskmanager` | `thesis-topk:local` | Flink operator execution |

The Python simulator executes from the host virtual environment during Docker E2E tests and connects through the exposed EMQX MQTT port `18884`. EMQX configuration is performed dynamically by `curl` calls before publishing.

### 7.2 Kubernetes Deployment

`k8s/pipeline.yaml` declares resources in namespace `thesis-streaming`:

| Resource | Type | Purpose |
| --- | --- | --- |
| Kafka | `StatefulSet` and `Service` | Persistent single-broker Kafka |
| EMQX | `Deployment`, `Service`, `ConfigMap` | MQTT and static Kafka action configuration |
| Sender | `Deployment` | Continuously publishes raw datasets with `thesis-simulator:local` |
| Flink configuration | `ConfigMap` | Flink and logging settings |
| Flink JobManager | `Job` and `Service` | Application-mode Kafka processing job |
| Flink TaskManager | `Deployment`, replicas `2` | Processing worker pool |

Key differences from Docker Compose:

| Concern | Docker Compose | Kubernetes |
| --- | --- | --- |
| Flink mode | Session cluster plus submitted bounded job | Application-mode Job |
| Sender execution | Host `.venv` script during E2E run | Dedicated simulator Deployment |
| EMQX rule setup | REST API configuration script | Static `ConfigMap` configuration |
| TaskManagers | 1 container in current Compose file | 2 replicas in manifest |
| Run behavior | Finite benchmark execution | Sender repeats continuously (`--repeat=0`) |

The Kubernetes manifests are configured for functionality and deployment testing; the latest result values in this report are from Docker Compose E2E validation, not from a Kubernetes performance run.

## 8. Automation And Operations

### 8.1 Command Interface

The `Justfile` is the primary user interface for build, test, execution, and inspection.

| Recipe | Function |
| --- | --- |
| `just venv` | Create/install Python simulator environment |
| `just simulator-test` | Dry-run sender preprocessing for synthetic and all raw datasets |
| `just image` | Build Flink application image |
| `just simulator-image` | Build Python sender image |
| `just setup` | Build and configure the Docker service stack |
| `just e2e` | Execute full MQTT-to-Flink benchmark |
| `just bench` | Execute algorithm-only benchmark |
| `just validate` | Validate E2E artifacts and live service state |
| `just monitor-bg` | Start monitor GUI in background |
| `just test-all` | Run test and benchmark suite |
| `just test-all-verbose` | Run suite while saving console evidence |
| `just k8s-apply` | Apply Kubernetes manifest |

### 8.2 End-To-End Test Procedure

The E2E benchmark performs this sequence:

1. Package the Flink Java application.
2. Reset and start clean Docker services.
3. Create Kafka topics and configure EMQX Kafka routes.
4. Create/use the simulator virtual environment.
5. Publish a fixed number of MQTT events.
6. Wait for Kafka offsets to confirm ingress completion.
7. Submit a bounded Flink Kafka job.
8. Count emitted `TopKResult` entries.
9. Persist timing and count reports.
10. Validate artifacts and live service state from the CLI.

This procedure verifies message conservation through the tested path: expected sender records equal Kafka records and Flink output count for the completed benchmark.

## 9. Monitoring And GUI

The monitor service in `scripts/monitor.py` exposes a realtime dashboard and JSON endpoints. It gathers data from:

- EMQX HTTP metrics for connector/action status and bridge success/failure counts;
- Kafka offsets for total and per-topic ingress counts;
- Flink REST `/overview` and `/jobs/overview`;
- Flink log output for result/error counts;
- algorithm benchmark report files;
- E2E CSV summaries.

### 9.1 Displayed Measurements

| Dashboard field | Basis |
| --- | --- |
| MQTT ingress | EMQX broker published count |
| Kafka ingress | Kafka topic offsets |
| Flink outgress | Count of emitted `TopKResult` rows in the Flink log |
| Bridge status | EMQX Kafka connector/actions |
| Ingestion completeness | Kafka records / expected records |
| Processing completeness | Flink result rows / Kafka records |
| Topic traffic | Individual Kafka topic offsets |
| Top-k agreement | Algorithm report agreement against internal exact baseline |
| Prune ratio | Algorithm benchmark result |
| Partitioned precision | Four-partition model, not distributed Spark accuracy |
| Shuffle proxy | Calculated candidate bytes, not Spark shuffle metric |

### 9.2 Test Execution From GUI

The GUI includes controls that execute CLI test profiles and stream output back to the browser:

| GUI action | Executed validation |
| --- | --- |
| Run Full CLI Tests | Unit tests, images, manifests, algorithm benchmark, Docker E2E, monitor and GUI smoke checks |
| Run Raw Topics E2E | Intel/Pump/Gas sender, three topic routes, Kafka/Flink record validation |

This keeps test execution reproducible through the same CLI scripts while exposing execution state to a user monitoring the system.

## 10. Validated Result Evidence

### 10.1 Raw Dataset End-To-End Execution

Latest tested flow:

```text
PythonSimulator
  -> EMQX MQTT
  -> EMQX Kafka producer actions
  -> Kafka topics: thesis.raw.intel, thesis.raw.pump, thesis.raw.gas
  -> Flink bounded Kafka source
  -> TopKResult
```

Test configuration:

| Parameter | Value |
| --- | ---: |
| Dataset | `all` |
| Events read per dataset | 5 |
| Total expected messages | 15 |
| Dimensions argument | 4 |
| `k` | 2 |
| Missing rate argument | 0.2 |
| Publisher target rate | 200 messages/s |
| MQTT QoS | 0 |

Result:

| Metric | Value | Validation |
| --- | ---: | --- |
| Kafka Intel records | 5 | Passed |
| Kafka Pump records | 5 | Passed |
| Kafka Gas records | 5 | Passed |
| Kafka total records | 15 | Equal to expected |
| Flink output records | 15 | Equal to Kafka input |
| Flink log errors | 0 | Passed |
| Finished Flink jobs | 1 | Passed |
| Failed Flink jobs | 0 | Passed |

Timing:

| Stage | Duration |
| --- | ---: |
| Simulator MQTT publication | 548 ms |
| MQTT publication start to Kafka completeness | 1,678 ms |
| Bounded Flink drain | 3,884 ms |
| Total E2E | 5,563 ms |
| E2E throughput | 2.70 messages/s |

### 10.2 Algorithm Benchmark Indicator

A separate synthetic algorithm run currently records:

| Metric | Value |
| --- | ---: |
| Objects | 100 |
| Queries | 2 |
| `k` | 10 |
| Missing rate | 0.35 |
| Exact baseline average | 7.801 ms |
| Certified-pruned average | 14.814 ms |
| Fast-candidate average | 2.512 ms |
| Fast candidate internal precision@k | 1.000 |
| Candidate communication-reduction proxy | 0.600 |
| Four-partition shuffle proxy | 5,120 bytes |

The benchmark is identified in its output as `executionEngine=java-local`, `partitionModelNodes=4`, and `shuffleMetric=calculated-candidate-proxy`. It is included as internal algorithm behavior evidence rather than as distributed Spark measurement.

## 11. Strengths Of The Current Implementation

1. **Separated ingestion responsibilities.** Raw file parsing and MQTT publication are isolated from Flink job processing.
2. **Dataset extensibility.** New sender data sources can implement a normalized event producer and attach to a topic mapping.
3. **Per-source observability.** Intel, Pump, and Gas records can be counted separately in MQTT/Kafka routing and the GUI.
4. **Scripted reproducibility.** Service setup, EMQX API creation, benchmarking, and validation are executable from CLI recipes.
5. **Deterministic E2E testing.** Bounded Kafka consumption provides finite test completion and message-count validation.
6. **Portable deployment definition.** Docker Compose handles local verification and Kubernetes manifests describe cluster deployment.
7. **Transparent metric labeling.** Algorithm proxy metrics are explicitly separated from measured distributed/Spark results.

## 12. Limitations And Risks

| Area | Current limitation | Consequence |
| --- | --- | --- |
| Flink scalability | Job forces `parallelism=1` in code | Kubernetes worker replicas are not used for parallel benchmark execution as currently configured |
| Delivery semantics | Raw E2E runs use MQTT QoS `0` | The benchmark validates observed successful records but does not demonstrate delivery guarantees under failures |
| Kafka topology | Single Kafka broker | No broker redundancy or failover evidence |
| EMQX topology | Single EMQX instance | No MQTT ingress high availability evidence |
| Kubernetes verification | Manifests exist but current recorded E2E result is Compose-based | No Kubernetes throughput or recovery result is currently claimed |
| Broker metric interpretation | Global EMQX `messages.dropped` is nonzero while Kafka actions succeed | Dashboard users must distinguish MQTT delivery behavior from bridge forwarding health |
| Imputation model | Simplified generated imputation rules | Not equivalent to dataset-specific DD imputation from the papers |
| Top-k semantics | PTD-style dominance score, not Topk-iDS PT-k semantics | Not comparable to paper F-score claims |
| Accuracy evaluation | No complete-data ground-truth injection/retention protocol | No paper-grade precision/recall/F-score result |
| Performance scale | Small smoke and prototype benchmark sizes | Does not establish paper-scale scalability |

## 13. Recommended Next Implementation Steps

### Operational Hardening

1. Remove the fixed `env.setParallelism(1)` behavior or make it configurable, then measure Flink parallel execution.
2. Define an intentional MQTT reliability profile (`QoS=1` for durable benchmark runs) and test reconnect/retry behavior.
3. Investigate the EMQX global dropped-delivery metric and document whether it represents absent MQTT subscribers or another broker condition.
4. Add Kubernetes E2E execution and reporting rather than manifest-only validation.
5. Add failure tests for broker restart, TaskManager loss, and malformed payload handling.

### Research Validation

1. Retain complete Intel/Pump/Gas source records and inject missing fields under a controlled protocol.
2. Implement dataset-specific DD rules and ground-truth Precision/Recall/F-score calculation.
3. Implement PT-k count-window evaluation if reproducing Topk-iDS results is required.
4. Implement aR-tree/MapReduce-style summaries and measured communication cost if reproducing distributed PTD is required.
5. Use actual Spark 3.0 execution and Spark metrics if validating pre-defense shuffle-write results is required.

## 14. Reproduction Commands

Create and test the Python sender environment:

```bash
just venv
just simulator-test
```

Build runtime images and validate configuration:

```bash
just image
just simulator-image
just config-check
```

Run synthetic full verification:

```bash
OBJECTS=100 QUERIES=2 DIMENSIONS=4 K=10 MISSING_RATE=0.35 \
RATE_PER_SECOND=200 QOS=0 WINDOW_MS=10000 \
just test-all-verbose
```

Run raw-topic E2E validation:

```bash
DATASET=all MAX_EVENTS=5 EXPECTED_MESSAGES=15 OBJECTS=5 QUERIES=1 \
DIMENSIONS=4 K=2 MISSING_RATE=0.2 RATE_PER_SECOND=200 QOS=0 \
just e2e-fast

DATASET=all OBJECTS=5 QUERIES=1 DIMENSIONS=4 K=2 MISSING_RATE=0.2 just bench
python3 scripts/validate-e2e.py --expected-messages 15 --expected-queries 1
```

Start realtime monitoring:

```bash
just monitor-bg
```

Dashboard endpoint:

```text
http://localhost:8088
```

Apply the Kubernetes definition after images are made available to the Kubernetes cluster:

```bash
just image
just simulator-image
just k8s-apply
```

## 15. Implementation Evidence Map

| Subject | Source artifact |
| --- | --- |
| Docker services | `docker-compose.e2e.yml` |
| Kubernetes deployment | `k8s/pipeline.yaml` |
| Flink application image | `Dockerfile` |
| Python sender image | `docker/simulator/Dockerfile` |
| Simulator preprocessing and publication | `scripts/simulator.py` |
| EMQX HTTP API configuration | `scripts/configure-emqx.sh` |
| E2E runner | `scripts/e2e-benchmark.sh` |
| Live dashboard and API | `scripts/monitor.py` |
| Strict validation | `scripts/validate-e2e.py` |
| CLI orchestration | `Justfile` |
| Flink Kafka job | `src/main/java/com/thesis/topk/flink/ProbabilisticTopKJob.java` |
| Imputation operator | `src/main/java/com/thesis/topk/flink/FlinkImputationFunction.java` |
| Window/top-k operator | `src/main/java/com/thesis/topk/flink/TopKProcessFunction.java` |
| Ranking algorithms | `src/main/java/com/thesis/topk/algorithm/` |
| Latest E2E summary | `reports/e2e/summary.md` and `reports/e2e/summary.csv` |
| Paper comparability analysis | `reports/validation/paper-alignment.md` |

## Conclusion

The current EMQX-Kafka-Flink implementation is a functioning and testable stream-processing prototype. It successfully preprocesses multiple incomplete-data sources, routes dataset-specific MQTT traffic into Kafka through EMQX, executes Flink imputation and top-k processing, and exposes reproducible validation and monitoring interfaces. The verified E2E result demonstrates complete passage of the current raw sample workload through the implemented path with no Kafka-action or Flink processing failures.

The system is suitable as the present implementation chapter foundation and as an experimental platform for subsequent thesis work. Claims should remain limited to the verified prototype and operational E2E results until the paper-specific algorithms, ground-truth accuracy protocol, large-scale workloads, and measured distributed metrics are implemented.
