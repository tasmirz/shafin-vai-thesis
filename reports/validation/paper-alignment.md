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
| DSCP pruning | reducer-safe lower-bound frontier: each local threshold is strengthened by the global k-th lower-bound frontier; pruning occurs only when `UB < tau` |
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
| Per-partition DSCP/AES execution | ICCIT Algorithm 1 | Implemented for named treatments; curated runs use selected index-level bounds, global lower-bound frontier propagation and exact partial-MBR traversal |

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

## Hadoop Comparison Boundary

`docker-compose.hadoop.yml` provides an isolated HDFS/YARN pseudo-distributed surface and
`scripts/research/validate_hadoop_cluster.sh` runs an Apache MapReduce smoke job against it.
The validated smoke execution completed one map and one reduce task, reporting `52` reduce
shuffle bytes and output counts for the deterministic `ptd baseline` / `ptd aes dscp` input;
its evidence is in `reports/hadoop/mapreduce-smoke.log`. This establishes reproducible Hadoop
infrastructure on the current machine. It is not an ICCIT
PTD timing: no Hadoop Mapper/Reducer implementation of the Rai-Lian/ICCIT algorithm has yet
been executed. A same-machine Hadoop-vs-Spark matrix is valid only after those PTD jobs use the
same curated objects, query set, `k`, partitions, repetitions and artifact protocol.

## Evaluation Metrics Alignment

This section outlines how the performance of the proposed framework (as presented in the ICCIT 2025 paper) maps to the benchmark logs generated by our system (`TopKBenchmark.java` and end-to-end tests). Evaluated using three principal metrics described in Equations (6)–(9).

### 1. Actual Wall Clock Time (`act WC`)
**Paper Definition:** `act WC = T_filter + T_refine`, averaged over queries.
**Framework Mapping:**
- Spark treatment `algorithmElapsedMs` is computed as
  `sum(filterMs + emissionMs + refineMs)` over saved queries. The exposed emission/reducer term
  is part of the distributed filtering stage and permits direct AES inspection.
- Input/index preparation and Spark/JVM scheduling are retained separately as `setupMs`; optional
  oracle execution is retained separately as `validationMs`.
- The older Java-only `avgExactMs` and `avgFastCandidateMs` remain diagnostic local comparisons,
  not formal AES/DSCP ablation evidence.
Named CSV and stream profiles now persist configuration, logs, checksums where applicable, and
exact-agreement evidence under `reports/runs/<run-id>/`.

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
- Validation-enabled indexed treatments record an oracle-backed `falsePrunes` audit for
  aggregate-R-tree candidate elimination; DSCP treatments additionally record their extension
  threshold. Performance-only suites do not convert an unexecuted oracle into a correctness claim.
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
index level uses a deterministic representative candidate-instance probe sample and penalizes
estimated reducer subtree traversal as a stand-in for the historical/uniform query calibration
described in the target paper. This is an explicit Spark adaptation of Rai and Lian's
historical-workload communication-cost choice.
The implementation now performs heap-ordered local aggregate-R-tree candidate traversal by
descending conservative upper-bound score and avoids expensive candidate emission/refinement
preparation once a local k-th lower-bound frontier excludes remaining candidates. Offline
persisted historical/uniform query-log calibration for exported-level selection remains required
before claiming a byte-for-byte target-paper baseline reproduction.
For Spark-scale execution, mapper bound computation broadcasts only compact aggregate-node
summaries. Full leaf/instance aR-tree payloads remain keyed by server partition and are joined
only for reducer partial-MBR traversal; this avoids the former full-index broadcast that exhausted
memory on the 98,451-object road input.

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

### Bounded Object-Level Trace Evidence

New saved runs capture a bounded sample of object-level decisions in `object-traces.csv`.
Each row records the query, object, partition, `LB`, `UB`, `tau`, DSCP decision, partial-MBR
reference count, expanded baseline emissions and AES emissions. This provides inspectable
DDR/MBR and AES evidence without making published-scale runs retain every intermediate record.
The website run drawer surfaces these trace rows for direct inspection.

### Performance Failure Diagnosis And Correction

The pre-fix published-scale road attempt at
`reports/failed-runs/iccit-road-full-traversalcost-20260527T040250Z-aborted/spark.log` did not
complete a query and consumed about `8.9 GiB`. Two issues caused that profile:

- candidate envelopes retained expanded partial-MBR work before candidate survival was known;
- the aggregate R-tree packed by one coordinate, producing stripe-shaped parent MBRs whose loose
  upper bounds prevented useful pruning.

The Spark path now emits reducer partial work lazily only for survivors, scores/counts partial
records in one pass, broadcasts summary trees only, uses `MEMORY_AND_DISK_SER`, executes with
the recorded `local[4]` profile, and spatially packs the aR-tree by STR grouping. DSCP now
propagates a reducer-safe global lower-bound frontier without computing exact final scores during
filtering.

The exact OSM fixture run `osm-str-packed-exact-20260527T072842Z` validates this corrected path:
it filtered `6 / 256` objects, emitted `7,887` compact records instead of `22,451` expanded
emissions, and recorded `exactAgreement=true` with `falsePrunes=0`.
The four-treatment CLI verification `osm-str-exact-suite-20260527T074100Z-*` also records
`exactAgreement=true` and `falsePrunes=0` for baseline, AES-only, DSCP-only and AES+DSCP on the
curated OSM smoke input.

### Completed Full Curated Smartphone Spark Treatments

The replacement suite `iccit-smartphone-str-20260527T073310Z-*` executes the curated smartphone
dataset (`750` uncertain objects, `207,860` instances over `20` queries), `k=10`, eight
partitions, and Spark `local[4]`.

| Treatment | Spark algorithm time | Reduction vs indexed Spark baseline | Candidate filtered | Emitted records | Shuffle bytes |
|---|---:|---:|---:|---:|---:|
| Baseline | 41,800 ms | 0.00% | 0.00% | 6,358,938 | 151,550,746 |
| AES-only | 37,050 ms | 11.36% | 0.00% | 1,659,913 | 104,940,242 |
| DSCP-only | 43,303 ms | -3.60% | 3.05% | 6,302,572 | 149,227,453 |
| AES + DSCP | 36,527 ms | 12.61% | 3.05% | 1,609,878 | 102,649,060 |

AES is beneficial on this input; DSCP filters few objects and its threshold overhead outweighs
its small emission saving when executed alone. This measured Spark reduction does not reproduce
the paper's `34.2%` Hadoop synthetic percentage.

### Completed Full Bangladesh OSM Road Spark Treatments

The completed suite `iccit-road-full-str-20260527T073041Z-*` executes `98,451` Bangladesh
road-MBR objects and `787,342` normalized probabilistic instances with one stored query,
`k=10`, eight partitions, and Spark `local[4]`.

| Treatment | Spark algorithm time | Reduction vs indexed Spark baseline | Candidate filtered | Emitted records | Shuffle bytes |
|---|---:|---:|---:|---:|---:|
| Baseline | 18,034 ms | 0.00% | 99.30% | 1,432,467 | 93,168,331 |
| AES-only | 13,728 ms | 23.88% | 99.30% | 44,400 | 76,746,071 |
| DSCP-only | 14,493 ms | 19.64% | 99.52% | 914,440 | 86,085,003 |
| AES + DSCP | 12,062 ms | 33.12% | 99.52% | 30,152 | 75,654,443 |

`Candidate filtered` includes the Rai-Lian indexed baseline's aR-tree bound filtering. DSCP's
additional effect is the difference from that baseline: it reduces survivors from `688` to
`472` and lowers expanded emission scope from `1,432,467` to `914,440`. The full method is
`33.12%` faster than the same-machine indexed Spark baseline for this OSM dataset.

Published Hadoop values remain reference values only. A Hadoop-vs-Spark engine comparison is not
valid until the genuine Rai-Lian/ICCIT PTD MapReduce jobs are implemented and executed against
these identical artifacts and configuration controls. Paper-style observed figures are generated
under `reports/figures/`, with Hadoop references kept in a separate table.
