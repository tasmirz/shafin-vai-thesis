# Probabilistic Top-k Spark Research Workbench Todo List

## 1. Project Scaffold

- [x] Create a Maven Java project for a Flink DataStream implementation.
- [x] Pin Java, Flink, JUnit, AssertJ, and logging dependencies.
- [x] Add executable entry points for the Flink job, simulator, and benchmark harness.

## 2. Paper-Grounded Domain Model

- [x] Define raw stream events with event time, query id, attributes, missing flags, and operation type.
- [x] Define differential-dependency-style imputation rules.
- [x] Define probabilistic instances emitted after imputation.
- [x] Define candidate scores, score bounds, and top-k result records.

## 3. Dataset Providers And Synthetic Data

- [x] Generate deterministic synthetic stream events with configurable object count, dimensions, missing-rate, query count, and seed.
- [x] Generate dummy imputation rules and query points.
- [x] Support imperfect data with missing attributes and probabilistic repairs.
- [x] Add a pluggable dataset provider interface.
- [x] Add a CSV provider so real datasets can be adapted into the synthetic-data maker later.

## 4. Core Algorithm

- [x] Implement missing-value imputation that emits probabilistic instances.
- [x] Implement dynamic dominance-style score computation relative to a query point.
- [x] Implement lower/upper bound estimation for pruning.
- [x] Implement local candidate pruning and exact refinement.
- [x] Implement top-k selection with deterministic tie-breaking.

## 5. Flink Streaming Job

- [x] Build a DataStream topology with event-time watermarks.
- [x] Add a keyed imputation stage.
- [x] Add a keyed candidate generation/refinement stage using state and event-time timers.
- [x] Emit continuous top-k results to stdout for local execution.

## 6. Testing

- [x] Add unit tests for imputation.
- [x] Add unit tests for dominance scoring and top-k ordering.
- [x] Add unit tests for pruning correctness against exact ranking.
- [x] Add unit tests for simulator determinism.

## 7. Benchmarking

- [x] Add a command-line benchmark that compares exact all-to-all ranking with pruned ranking.
- [x] Report records processed, candidate count, prune ratio, exact runtime, pruned runtime, and top-k agreement.
- [x] Report Spark 3.0/4-node-aligned partitioned candidate refinement, shuffle-write proxy bytes, communication reduction, and precision@k.

## 8. Verification

- [x] Run the full Maven test suite.
- [x] Run the benchmark harness on dummy data.
- [x] Fix any compile, test, or benchmark failures.
- [x] Convert stream-facing records to Flink-friendly POJOs after Java 25 exposed Kryo/module-access issues.
- [x] Run Dockerized MQTT -> EMQX -> Kafka -> Spark E2E validation from the CLI.

## 9. Reproducible Spark Input And Benchmark Profiles

- [x] Keep the MQTT -> EMQX Kafka sink -> Kafka -> Spark pipeline as the stream-ingress path.
- [x] Replace driver-side Kafka draining with bounded Spark Structured Streaming ingestion.
- [x] Add a deterministic CSV-to-Spark integration fixture and executable test profile.
- [x] Add a finite MQTT/Kafka/Spark E2E streaming test profile.
- [x] Persist named run manifests, configuration parameters, dataset checksums, metrics, and logs.
- [x] Add saved-run comparison output with benchmark-hygiene warnings.

## 10. PTD-BenchLab Website

- [x] Build a research dashboard and navigation shell described in `papers/Website Guide.md`.
- [x] Add saved-run browsing and fair-comparison views backed by `reports/runs/*` artifacts.
- [x] Add raw CSV inspection, schema summary, missing-value visibility, and quality status.
- [x] Add launch forms for deterministic CSV and finite MQTT/Kafka/Spark validation profiles.
- [x] Add exact-validation status, measured runtime/pruning analytics, log drill-down, and bundle export.
- [ ] Add full synthetic smartphone and Bangladesh road/OSM dataset simulator screens.
- [ ] Surface the tracked Bangladesh OSM source-readiness manifest and road-to-MBR curation status in the website.
- [ ] Add probability normalization audit after paper-style uncertain-instance datasets are available.
- [ ] Add experiment-matrix, ablation and academic LaTeX report generation screens.
- [ ] Add detailed MQTT/Kafka/Spark telemetry for traces, actual shuffle bytes and partition skew.

## 11. Paper-Faithful Experimental Expansion

- [x] Implement selectable `baseline`, `AES-only`, `DSCP-only`, and `AES+DSCP` Spark variants for ablation.
- [x] Persist selected treatment, executed emission counts, AER, and DSCP false-prune audit in saved runs.
- [x] Remove global exact-score-derived filtering bounds; execute conservative partition-local bounds and same-partition emissions.
- [x] Add a validated Bangladesh OSM source protocol and CLI readiness check without committing raw geospatial inputs.
- [ ] Measure actual Spark shuffle records/bytes and phase timing rather than only payload proxies.
- [ ] Curate Bangladesh OSM line segments into projected MBRs with 5-11 normalized instances and a partition-index manifest.
- [ ] Add paper-shaped smartphone uncertain-instance/probability data and Spark provider for curated uncertain-object rows.
- [ ] Evaluate dynamic load balancing and worker-utilization/skew metrics inspired by `papers/3700838.3700859.pdf` after PTD spatial semantics are established.
