# Baseline vs ICCIT Hadoop vs Spark Treatment Comparison Map

This repo's comparison target is:

1. Baseline distributed PTD control.
2. ICCIT Hadoop AES-only, DSCP-only, and AES+DSCP reference treatments.
3. This repo's Spark AES-only, DSCP-only, and AES+DSCP treatments.

The canonical name is `DSCP`. The CLI and web API also accept `dhcp` spellings as aliases,
but saved artifacts and code use `dscp`.

## Implementation Status

| Comparison group | Current status | Implementation source |
|---|---|---|
| Baseline control | Implemented as Spark `baseline`; AES=false and DSCP=false. | `src/main/java/com/thesis/topk/algorithm/variant/PtdAlgorithmRegistry.java`, `src/main/java/com/thesis/topk/spark/SparkTopKEngine.java` |
| ICCIT Hadoop AES/DSCP/AES+DSCP | Published reference values only. There is no executable Hadoop PTD MapReduce implementation of these algorithms yet. | Reference constants in `scripts/research/render_publication_report.py` and web constants in `web/server.py`; Hadoop infrastructure smoke only in `scripts/research/validate_hadoop_cluster.sh` |
| Spark AES/DSCP/AES+DSCP | Implemented and runnable as `aes-only`, `dscp-only`, and `aes-dscp`. | `PtdAlgorithmRegistry.java`, `SparkTopKEngine.java`, `src/main/java/com/thesis/topk/spark/ProbabilisticTopKSparkJob.java` |

## Logic Ownership

| Concern | File(s) | Notes |
|---|---|---|
| Treatment registry and aliases | `src/main/java/com/thesis/topk/algorithm/variant/PtdAlgorithmRegistry.java` | Defines `baseline`, `aes-only`, `dscp-only`, `aes-dscp`; accepts `aes+dhcp` and `dhcp-only` aliases. |
| Spark ranking pipeline | `src/main/java/com/thesis/topk/spark/SparkTopKEngine.java` | Builds probabilistic instances, partitions objects, builds aggregate R-trees for MBR inputs, applies optional DSCP filtering, emits expanded or AES-aggregated records, refines survivors exactly. |
| Spark CLI job | `src/main/java/com/thesis/topk/spark/ProbabilisticTopKSparkJob.java` | Parses run arguments, selects treatment, executes Spark engine, prints metrics consumed by archive scripts. |
| Exact local oracle | `src/main/java/com/thesis/topk/benchmark/TopKBenchmark.java` | Used for validation-scale agreement checks, not the Spark performance path. |
| Saved run parser/archive | `scripts/research/archive_run.py` | Converts Spark logs into `metrics.json`, `metrics.csv`, `manifest.json`, object traces, and summary fields. |
| Four-treatment Spark suite | `scripts/research/run_ablation_suite.sh` | Runs `baseline`, `aes-only`, `dscp-only`, and `aes-dscp` on the same input and checks fairness/consistency invariants. |
| Curated paper setup runner | `scripts/research/run_paper_setup.sh` | Runs named paper-style baseline, Spark AES+DSCP upgrade, paired setup, or full ablation. |
| ICCIT/Spark comparison launcher | `scripts/research/run_iccit_comparison_suite.sh` | Runs full four-treatment Spark comparison for curated profiles and writes validation markdown. |
| Publication matrix renderer | `scripts/research/render_publication_report.py` | Contains published ICCIT Hadoop reference times for baseline/AES/DSCP/AES+DSCP and combines them with observed Spark suite metrics. |
| Multi-dataset renderer | `scripts/research/render_dataset_benchmark_report.py` | Builds all-dataset Spark treatment and stream matrices from saved run artifacts. |
| Web API and launcher | `web/server.py` | Exposes saved runs, treatment comparison matrix, all-dataset report, launch modes, and reference constants. |
| Web UI | `web/static/index.html`, `web/static/app.js` | Displays launch forms and comparison tables for Hadoop references and Spark treatment suites. |

## Metrics Compared

| Metric | Spark source | Hadoop reference availability |
|---|---|---|
| Clock time | `algorithmElapsedMs` in archived Spark metrics. It excludes setup and validation time. | Published ICCIT wall-clock treatment times are stored as reference constants. |
| Pruning rate | `avgPruneRatio`, per-query `pruneRatio`, `refined`, and `pruned` from `SparkTopKEngine.QueryRanking`. | Not available in the current published-reference constants. |
| Emitted records / AES effect | `totalEmittedRecords`, `totalBaselineEmissions`, `totalAesEmissions`, and AER from archived Spark metrics. | Not available in the current published-reference constants. |
| Shuffle bytes/records | Spark listener metrics through `SparkTaskMetrics` and archived summary fields. | Not available in the current published-reference constants. |
| Correctness | `exactTopKAgreement` and `falsePruneCount` from validation-enabled runs. | Not available from the published ICCIT timing table. |

## How To Run Today

Run a small deterministic CSV Spark treatment:

```bash
RUN_ID=csv-check ALGORITHM=aes-dscp tests/integration/test_csv_spark.sh
```

Run all four Spark treatments on the deterministic fixture:

```bash
SUITE_ID=ablation-check BUILD_IMAGE=1 tests/integration/test_csv_ablation.sh
```

Run a curated ICCIT-style Spark comparison profile:

```bash
PROFILE=smartphone SUITE_ID=smartphone-suite BUILD_IMAGE=1 just iccit-compare
```

Run the paired baseline-vs-Spark-AES+DSCP guard that also checks the Hadoop reference boundary:

```bash
SUITE_ID=hadoop-reference-check BUILD_IMAGE=1 just hadoop-aes-dscp-test
```

Validate only the Hadoop cluster infrastructure:

```bash
just hadoop-smoke
```

Important: `just hadoop-smoke` does not run PTD baseline, AES, DSCP, or AES+DSCP Hadoop
algorithms. It validates HDFS/YARN/MapReduce infrastructure only.

Render publication comparison artifacts from completed suites:

```bash
python3 scripts/research/render_publication_report.py \
  --smartphone-suite iccit-smartphone-str-20260527T073310Z \
  --road-suite iccit-road-full-20q-20260527T094500Z
```

Start the web UI:

```bash
just web
```

The web UI currently exposes:

- CSV and stream launchers.
- Paper setup launcher.
- Hadoop-reference AES+DSCP guard launcher.
- Treatment comparison table: ICCIT Hadoop published references plus complete Spark four-treatment suites discovered from `reports/runs`.
- All-dataset comparison table from `reports/publication/all-dataset-benchmark-report.json`.

## Verification Finding

The Spark side has executable baseline/AES/DSCP/AES+DSCP treatments. The Hadoop side does not
yet have executable PTD MapReduce treatment jobs in this repo. Therefore a valid final
engine-to-engine comparison still requires implementing Hadoop PTD jobs for the same four
treatments on the same input artifacts and hardware. Until then, Hadoop rows are reference
context and Spark rows are runnable current-repo evidence.
