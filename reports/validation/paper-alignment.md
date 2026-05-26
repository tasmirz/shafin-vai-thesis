# Paper Alignment Validation Report

This report describes the Spark-upgraded project state.

## Source Paper Alignment

The supplied 2025 ICCIT work proposes a distributed PTD framework with:

- Dynamic Smart Candidate Pruning (DSCP)
- Aggregated Emission Strategy (AES)
- distributed filtering/refinement phases
- Hadoop/MapReduce execution in the original paper
- future extension potential on Apache Spark

This repository implements that upgrade direction by using Apache Spark as the distributed execution layer.

## Implemented Spark Mapping

| Paper idea | Spark project mapping |
|---|---|
| uncertain objects and probabilistic instances | raw events repaired into `ProbabilisticInstance` objects; curated CSV rows retain supplied appearance mass and instance identity |
| DDR-style query-relative dominance | `DominanceScorer` and query-aware ranking |
| object-to-server distribution | curated data retains seeded random object-level `serverId`; raw streams fall back to stable hashing |
| LB/UB candidate bounds | curated inputs: distributed aggregate R-tree exported-level DDR/MBR bounds and partial-node reducer traversal; inputs without MBRs: conservative remote-mass upper allowance; no global exact score in filtering |
| DSCP pruning | per-partition k-th lower-bound threshold, pruning only when `UB < tau` |
| AES compact emissions | for MBR inputs, one candidate/destination record groups partial index-node references; controls emit one record per partial reference; legacy non-MBR inputs retain competitor-set emissions |
| exact refinement | MBR inputs combine exact local/full-node mass with reducer-traversed partial MBR mass; non-MBR survivors are rescored exactly |

Every saved run now declares its bound mode. Curated MBR artifacts execute
`rai-lian-artree-selected-level-partial-reducer`; stream or legacy inputs lacking MBR metadata execute
`conservative-remote-mass-no-mbr`. This corrects the earlier prototype issue in which filtering
used a globally exact score and makes zero-pruning legacy results distinguishable from
MBR-backed DSCP evidence.

## Baseline And Upgrade Match Gate

| Required experimental element | Evidence in the local papers | Current state |
|---|---|---|
| Random object-level assignment to `N` server partitions | Rai and Lian (2023), real/synthetic setup | Implemented in generated manifests and consumed by the Spark CSV provider |
| One spatial index/summary over each server partition | Rai and Lian (2023), offline preprocessing | Implemented: packed aggregate R-tree per Spark server partition, selected exported level, and reducer traversal for partial node references |
| Real road uncertainty regions represented as MBRs | Baseline uses 98,451 California road MBRs; upgrade uses Bangladesh road segments | Implemented for 98,451 Bangladesh line-feature MBR objects in EPSG:9678 |
| Uniform samples with normalized probabilities | Both paper dataset descriptions | Implemented and audited in generated CSV artifacts |
| Smartphone uncertain-object dataset | ICCIT upgrade experiment | Implemented with normalized price/inverse-battery instances and appearance probabilities |
| Uniform/Gaussian/Zipf generic synthetic distributions and `lmax` | Rai and Lian (2023) | Implemented by `build_paper_dataset.py synthetic`, including Zipf skewness 0.8 and normalized sampled instances |
| Baseline, AES-only, DSCP-only, AES+DSCP | ICCIT upgrade ablation | Implemented treatment selection and saved comparisons |
| Filtering not performing final global scoring | Two-phase framework in both papers | Corrected: global exact scoring occurs only during refinement/oracle validation |
| Per-partition DSCP/AES execution | ICCIT Algorithm 1 | Implemented for named treatments; curated runs use selected index-level bounds and exact partial-MBR traversal |

## Bangladesh Source Preparation

The supplied HOTOSM/OpenStreetMap line shapefile contains `494,717` `LINESTRING` features from a
recorded `2026-05-10` snapshot, exceeding the baseline paper's `98,451` road-MBR scale. The
tracked protocol is `config/research/bangladesh-osm-replication.json`; the non-destructive
command `just osm-prepare-check` verifies provenance and configuration readiness.
`scripts/research/build_paper_dataset.py osm` curated a local baseline-scale artifact with
`98,451` projected MBR objects and `787,342` normalized instances in EPSG:9678; its tracked
index/validation manifest is `reports/datasets/bangladesh-road-paper.json`. The large CSV is
kept under ignored `datasets-curated/` and can be regenerated from the manifest parameters.

## Supporting-Paper Adoption Boundary

`papers/3700838.3700859.pdf` addresses distributed uncertain skyline enumeration rather than
probabilistic top-k domination. Its dynamic load-balancing and worker-utilization evaluation
are adopted as observability ideas: generated manifests record partition load and finite Spark
runs record task counts, executor/GC timing and straggler ratios. Its skyline scoring semantics
are not substituted into PTD.

## Runtime Validation Surface

The full runtime path is now:

```text
PythonSimulator -> EMQX MQTT -> Kafka -> Apache Spark Structured Streaming bounded reader -> SparkTopKEngine -> TopKResult
```

Validation commands:

```bash
just spark
just image
just setup
just spark-submit
just e2e
just validate
just test-all
```

## Deployment Validation Surface

Updated files:

- `Dockerfile`
- `Dockerfile.spark`
- `docker-compose.e2e.yml`
- `k8s/pipeline.yaml`
- `scripts/setup-services.sh`
- `scripts/e2e-benchmark.sh`
- `scripts/validate-e2e.py`
- `scripts/monitor.py`
- `Justfile`

## Notes

The older Flink package remains available only as reference code. The deployment and reproducibility path is Spark-first.

## Evaluation Metrics Alignment

This section outlines how the performance of the proposed framework (as presented in the ICCIT 2025 paper) maps to the benchmark logs generated by our system (`TopKBenchmark.java` and end-to-end tests). Evaluated using three principal metrics described in Equations (6)–(9).

### 1. Actual Wall Clock Time (`act WC`)
**Paper Definition:** `act WC = T_filter + T_refine`, averaged over queries.
**Framework Mapping:**
- `avgExactMs` logs map to the **Baseline** (`act WC`) without bounds.
- `avgFastCandidateMs` logs are a candidate-pruning comparison metric; it is not yet a formal
  AES+DSCP ablation result.
Our runtime benchmark harness records filtering, emission and refinement timing, but it does not
by itself reproduce the paper's reported percentages. Named CSV and stream profiles now persist
configuration, logs, checksums where applicable, and exact-agreement evidence under
`reports/runs/<run-id>/`.

### 2. Communication Cost (`act CC`)
**Paper Definition:** `act CC = O(|S_cand| · |t| · |tj.list|)`, defining filtering mapper emission data payload sizes.
**Framework Mapping:**
- Spark treatment runs record observed `shuffleBytes` and `shuffleRecords` through task metrics,
  alongside emissions and AER. The older Java-only harness proxy remains diagnostic only.

### 3. Aggregated Emission Rate (`AER`) / Ablation
**Paper Definition:** `AER = E_AES / E_baseline × 100`. Isolates DSCP candidate eliminations and AES grouped network packet compressions.
**Framework Mapping:**
- Selectable `baseline`, `dscp-only`, `aes-only`, and `aes-dscp` treatments execute through the
  same Spark pipeline.
- Non-AES treatments materialize instance-competitor emissions; AES treatments materialize one
  aggregated competitor record per surviving instance. Saved runs record emitted records and AER.
- DSCP treatments record thresholds, pruning ratios, and an oracle-backed `falsePrunes` audit.
- These are Spark treatment implementations with paper-shaped dataset artifacts and observed
  runtime metrics; reproduction of paper percentages still requires running the controlled
  full-scale suite and does not follow merely from data curation.

Validation-enabled Spark runs emit `validationPerformed=true exactAgreement=true` only after an
oracle comparison; the attached local benchmark also records `topKAgreement`. These checks
support exactness for the tested finite inputs without claiming full paper reproduction.

### Historical MBR Pruning Regression Evidence

The deterministic fixture `tests/fixtures/paper/mbr-pruning.csv` places six non-overlapping MBR
objects around one query with `k=2`. The saved suite
`mbr-pruning-20260526T145955Z-468040-*` executed the four treatment variants under identical
configuration:

| Treatment | Bound mode | Pruned | Emitted records | Exact agreement | False prunes |
|---|---|---:|---:|---|---:|
| baseline | `ddr-mbr-full-possible` | not applicable | 30 | true | not applicable |
| AES-only | `ddr-mbr-full-possible` | not applicable | 6 | true | not applicable |
| DSCP-only | `ddr-mbr-full-possible` | 4 / 6 (66.67%) | 10 | true | 0 |
| AES + DSCP | `ddr-mbr-full-possible` | 4 / 6 (66.67%) | 2 | true | 0 |

Previously saved rows reporting `partition-local-conservative-no-mbr` and `0.0%` retain their
historical meaning: their loose safe upper bounds did not prove `UB < tau`. The website now
reports pruning as not applicable for variants that do not enable DSCP and exposes each run's
bound mode in the evidence drawer.

The curated road smoke run `osm-mbr-smoke-20260526T151737Z-477089` uses 40 Bangladesh
road-MBR objects and confirms the spatial path: `aes-dscp` pruned `16 / 40` objects (`40.0%`),
materialized 188 AES emissions against 1,784 expanded emissions (`AER=10.54%`), and passed
exact-oracle validation with zero false prunes. This is pipeline validation at smoke scale; the
98,451-object artifact remains the input for the full replication benchmark.

Those saved runs predate the distributed aggregate-R-tree implementation. New indexed execution
evidence is generated by `PROFILE=road-smoke VALIDATE_EXACT=true just iccit-compare`; it records
`partialMbrRefs` and bound mode `rai-lian-artree-selected-level-partial-reducer`. The selected
index level uses a deterministic representative candidate-instance probe sample as a stand-in
for the historical/uniform query calibration described in the target paper. This is an explicit
Spark adaptation of Rai and Lian's historical-workload communication-cost choice.
The implementation is not yet a byte-for-byte reconstruction of the target paper's
heap-ordered mapper traversal or its offline persisted historical/uniform query-log calibration;
those remain requirements before claiming a full target-paper baseline reproduction.

### Distributed Aggregate-R-Tree Execution Evidence

The validated suite `artree-road-smoke-20260526T222554Z-588509-*` executes 40 partitioned
Bangladesh road-MBR objects under the new index path with `k=5` and four partitions:

| Treatment | Bound mode | Partial MBR refs | Pruned | Exact agreement | False prunes |
|---|---|---:|---:|---|---:|
| baseline | `rai-lian-artree-selected-level-partial-reducer` | 850 | n/a | true | 0 |
| AES-only | `rai-lian-artree-selected-level-partial-reducer` | 850 | n/a | true | 0 |
| DSCP-only | `rai-lian-artree-selected-level-partial-reducer` | 850 | 1 / 40 | true | 0 |
| AES + DSCP | `rai-lian-artree-selected-level-partial-reducer` | 850 | 1 / 40 | true | 0 |

This suite proves reducer-side partial-MBR traversal and DSCP exactness on a real-source road
sample. It does not demonstrate an AES reduction: each selected remote summary contributes only
one partial reference per candidate/destination in this small workload, so aggregation has no
records to collapse (`AER=100%`). AES performance claims therefore remain conditional on
controlled workloads selecting multiple partial node references.
