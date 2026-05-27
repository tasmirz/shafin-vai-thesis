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
- [x] Add full synthetic smartphone and Bangladesh road/OSM dataset simulator screens.
- [x] Surface the tracked Bangladesh OSM source-readiness manifest and road-to-MBR curation status in the website.
- [x] Add probability normalization audit after paper-style uncertain-instance datasets are available.
- [x] Add experiment-matrix, ablation and academic LaTeX report generation screens.
- [x] Add detailed MQTT/Kafka/Spark telemetry for phase traces, actual shuffle bytes and partition skew indicators.

## 11. Paper-Faithful Experimental Expansion

- [x] Implement selectable `baseline`, `AES-only`, `DSCP-only`, and `AES+DSCP` Spark variants for ablation.
- [x] Persist selected treatment, executed emission counts, AER, and DSCP false-prune audit in saved runs.
- [x] Remove global exact-score-derived filtering bounds; execute conservative partition-local bounds and same-partition emissions.
- [x] Add a validated Bangladesh OSM source protocol and CLI readiness check without committing raw geospatial inputs.
- [x] Measure actual Spark shuffle records/bytes and phase timing rather than only payload proxies.
- [x] Curate Bangladesh OSM line segments into projected MBRs with 5-11 normalized instances and a partition-index manifest.
- [x] Add paper-shaped smartphone uncertain-instance/probability data and Spark provider for curated uncertain-object rows.
- [x] Evaluate dynamic load balancing and worker-utilization/skew metrics inspired by `papers/3700838.3700859.pdf` using partition manifests and observed Spark straggler ratios.
- [x] Persist bounded per-object DDR/MBR decision and AES emission trace samples for each saved Spark run.
- [x] Generate paper-style observed-treatment figures while separating published Hadoop reference values.
- [x] Add a curated paper-setup launcher for the Spark Rai-Lian indexed baseline, ICCIT AES+DSCP upgrade, paired comparison and full ablation.
- [x] Generate a publication-formatted performance/consistency report with CSV and LaTeX tables from immutable Spark suites.
- [x] Curate the supplied California/TIGER road source with declared CRS conversion and a fixed 20-query sidecar.
- [x] Preserve paper uncertain-object probability/partition/MBR fields over MQTT -> Kafka -> Spark and validate exact road-subset transport runs.
- [x] Compile multi-dataset CSV and stream benchmark matrices under `reports/publication/`.
- [ ] Execute the California/TIGER full-object fixed 20-query treatment suite; the archived one-query suite is supplemental exploratory evidence only.

## 12. Full Baseline Reproduction Boundary

- [x] Add a same-machine ICCIT treatment comparison profile with saved reports and declared assumptions for undisclosed controls.
- [x] Separate exact-validation fixtures from paper-sized performance executions so oracle cost is not presented as algorithm cost.
- [x] Implement Rai-Lian distributed aggregate R-tree level selection/cost model and reducer-side partial-MBR refinement for curated MBR inputs.
- [x] Add Rai-Lian paper-shaped synthetic uncertain-region datasets for uniform, Gaussian and Zipf (skew 0.8) centers with configurable `lmax`.
- [x] Add isolated Docker Compose HDFS/YARN MapReduce infrastructure and a non-PTD smoke validation command for future same-machine comparison.
- [x] Execute the full curated smartphone Spark treatment suite with 20 saved query results and trace artifacts.
- [x] Add heap-ordered local aR-tree candidate traversal for Rai-Lian mapper-side score-bound filtering.
- [ ] Add offline persisted historical/uniform query-log calibration if a byte-faithful Rai-Lian exported-level selection baseline is required.
- [ ] Implement the Rai-Lian/ICCIT algorithm as genuine Hadoop MapReduce jobs before labeling any Hadoop-vs-Spark timing as an engine comparison.
- [x] Execute the 98,451-object Bangladesh OSM four-treatment suite over a fixed 20-query set; archived under `iccit-road-full-20q-20260527T094500Z-*`.
- [ ] Add repetitions and confidence intervals before using the fixed 20-query Bangladesh suite as a final statistical paper-comparable result.
- [x] Complete the 98,451-object Bangladesh road Spark treatment suite using heap-ordered local
  aR-tree traversal, STR spatial packing and lazy reducer partial scoring. The completed
  four-treatment suite is archived under `iccit-road-full-str-20260527T073041Z-*`; the
  replacement smartphone suite is archived under `iccit-smartphone-str-20260527T073310Z-*`.
  The aborted pre-fix run is retained only as diagnostic history at
  `reports/failed-runs/iccit-road-full-traversalcost-20260527T040250Z-aborted/spark.log`.
