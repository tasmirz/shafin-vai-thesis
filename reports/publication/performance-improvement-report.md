# Performance Improvement and Experimental Consistency Report

Generated: 2026-05-27T14:08:30.978868+00:00

## Scope And Claim Boundary

This report evaluates the implemented Spark extension against an indexed Spark control on
the same machine and stored inputs. It does not report Hadoop-to-Spark speedup. The ICCIT
numbers below are published Hadoop reference values; Rai and Lian's baseline was also a
Hadoop/MapReduce study executed on different hardware and, for real data, California roads.

The Spark control is the implemented Rai-Lian-style distributed aggregate R-tree treatment
with selected exported levels and partial-MBR reducer traversal, with AES and DSCP disabled.
The Spark upgrade enables the ICCIT AES and DSCP extensions on that same indexed path.

## Source-Paper Experimental Reference

| Dataset | ICCIT Hadoop baseline | ICCIT Hadoop AES-only | ICCIT Hadoop DSCP-only | ICCIT Hadoop AES+DSCP | Published full reduction | act_CC |
|---|---:|---:|---:|---:|---:|---:|
| Synthetic smartphone | 66,520 ms | 44,760 ms | 58,484 ms | 43,757 ms | 34.2% | 1.4179 x 10^11 |
| Bangladesh OSM road | 56,274 ms | 42,967 ms | 46,848 ms | 42,366 ms | 24.7% | 1.543 x 10^10 |

ICCIT states that its measurements use pseudo-distributed Hadoop on Windows 11 with an
Intel Core i5-8265U, 8 GB RAM, Java 1.8, and 20 random queries. Rai and Lian evaluate
distributed PTD on ten Dell PowerEdge R730 servers using Java 1.8 and Hadoop 1.2.1,
with California road MBRs and synthetic uniform/Gaussian/Zipf distributions. ICCIT's
real-data label refers to its Bangladesh road dataset.

## Current Spark Experimental Protocol

| Input | Suite ID | Objects/query | Instances/events | Queries | k | Partitions | Control | Upgrade |
|---|---|---:|---:|---:|---:|---:|---|---|
| Synthetic smartphone | `iccit-smartphone-str-20260527T073310Z` | 750 | 207,860 | 20 | 10 | 8 | `baseline` | `aes-dscp` |
| Bangladesh OSM road | `iccit-road-full-20q-20260527T094500Z` | 98,451 | 787,342 | 20 | 10 | 8 | `baseline` | `aes-dscp` |

All four variants in each saved Spark suite use the same CSV checksum, `k`, and partition
count. `algorithmElapsedMs` is filtering plus emission plus refinement time; index/data
setup and optional exact-oracle validation are recorded separately.

## Observed Same-Machine Spark Performance

| Dataset | Spark indexed baseline | AES-only | DSCP-only | AES+DSCP | AES+DSCP reduction |
|---|---:|---:|---:|---:|---:|
| Synthetic smartphone | 41,800 ms | 37,050 ms (11.36%) | 43,303 ms (-3.60%) | 36,527 ms (12.61%) | 12.61% |
| Bangladesh OSM road | 917,176 ms | 519,036 ms (43.41%) | 603,290 ms (34.22%) | 379,637 ms (58.61%) | 58.61% |

| Dataset | Emitted records: baseline to full | Emission reduction | Shuffle bytes: baseline to full | Shuffle reduction | Indexed filtered: baseline to full |
|---|---:|---:|---:|---:|---:|
| Synthetic smartphone | 6,358,938 to 1,609,878 | 74.68% | 151,550,746 to 102,649,060 | 32.27% | 0.00% to 3.05% |
| Bangladesh OSM road | 123,247,562 to 1,406,544 | 98.86% | 3,230,578,777 to 1,622,996,652 | 49.76% | 97.94% to 98.88% |

### Interpretation

- Bangladesh OSM: AES+DSCP reduces measured Spark algorithm time by `58.61%`, emissions by `98.86%`, and shuffle bytes by `49.76%` against the indexed Spark control.
- Synthetic smartphone: AES+DSCP reduces time by `12.61%`; AES is beneficial, while DSCP-only is `3.60%` slower because it filters only a small fraction of indexed candidates on this input.
- The road reduction is similar in scale to the ICCIT improvement, but it is not a
  reproduction of ICCIT runtime because the executor, data provenance, and protocol differ.

## Consistency Audit

| Severity | Finding | Publication handling |
|---|---|---|
| High | ICCIT describes AES as collapsing redundant emissions and defines AER, but also states that baseline and proposed emitted the same number of records and leaves `act_CC` unchanged. | Report actual emitted records and shuffle bytes for the Spark implementation; do not claim reproduction of the ICCIT communication result. |
| High | Rai-Lian uses California road MBRs; ICCIT and the primary implementation report use Bangladesh road geometries. A supplemental supplied California/TIGER artifact matches the 98,451-object scale but is not established as Rai-Lian's exact source file. | Label the primary result `Bangladesh OSM road`; label California/TIGER results as supplemental scale-matched evidence only. |
| High | The published methods execute Hadoop MapReduce under different hardware conditions; the implemented treatments execute Spark `local[4]`. | Make only within-Spark treatment claims until genuine Hadoop PTD jobs run on identical inputs and hardware. |
| Medium | Rai-Lian selects distributed aR-tree levels through an historical/uniform-query cost model; this Spark implementation selects levels through deterministic bounded probes and reducer traversal estimates. | Describe it as a Rai-Lian-style Spark adaptation, not a byte-for-byte reproduction. |
| Medium | Full-scale performance runs set exact-oracle validation off; exact agreement and zero false-prune evidence comes from validation-enabled finite OSM suites. | Do not claim full-scale exactness until validation or an auditable sampled-validation protocol is run. |

## Implementation Alignment Evidence

| Paper mechanism | Implemented evidence | Status |
|---|---|---|
| Partition aggregate R-trees and distributed summaries | `AggregateRTree.build(...)`, `summaryOnly()`; Spark broadcasts summaries and keeps full reducer indexes keyed by partition. | Implemented adaptation |
| Selected exported aR-tree level | `selectExportLevel(...)` evaluates exported-node, partial-reference, and traversal costs. | Implemented with deterministic probe calibration |
| Partial-MBR reducer traversal | Spark joins partial work against reducer indexes and computes exact partial contribution. | Implemented |
| Rai-Lian-style control without extensions | `baseline` uses indexed path with AES=false and DSCP=false. | Implemented |
| ICCIT ablations | `aes-only`, `dscp-only`, and `aes-dscp` are saved comparable treatments. | Implemented |
| Paper-scale real input | Bangladesh OSM curated artifact contains 98,451 uncertain MBR objects. | Implemented for Bangladesh, not California |
| Hadoop engine reproduction | PTD MapReduce job implementing the same treatments on identical artifacts. | Not implemented |

## Validation Evidence

The performance suites above do not run the brute-force oracle. Exactness evidence currently
comes from validation-enabled OSM runs:

- `osm-str-packed-exact-20260527T072842Z`: AES+DSCP on 256 objects, exact agreement true, zero false prunes.
- `osm-str-exact-suite-20260527T074100Z-*`: all four variants on the finite OSM fixture, exact agreement true, zero false prunes.
- `paper-setup-role-exact-20260527T092000Z-*`: named Spark indexed baseline versus AES+DSCP, exact agreement true, zero false prunes.

## Publication Readiness Decision

**Publication-formatted preliminary Spark extension result:** supported.

**Paper-number reproduction or Hadoop-vs-Spark speedup claim:** not yet supported.

Required experiments before a final comparison claim:

1. Repeat treatments sufficiently for variance/confidence interval reporting on the same machine.
2. Run validation on tractable matched subsets and define the full-scale correctness audit protocol.
3. Implement and execute equivalent Hadoop PTD treatments on the same input artifacts before comparing engines.

## Manuscript-Ready Results Paragraph

On the curated Spark smartphone workload (750 uncertain objects represented by 207,860 probabilistic instances over 20 fixed queries), the indexed control completed in 41,800 ms, while AES+DSCP completed in 36,527 ms, a 12.61% reduction. On the 98,451-object Bangladesh OSM road workload, the indexed control completed in 917,176 ms and AES+DSCP completed in 379,637 ms, a 58.61% reduction for the fixed 20-query set. On road data, AES+DSCP also reduced emitted records by 98.86% and measured Spark shuffle bytes by 49.76%. These values quantify within-Spark algorithm treatment effects and are not direct runtime comparisons with the published Hadoop studies.

## Generated Artifacts

- `reports/publication/spark-treatment-matrix.csv`: machine-readable observed table.
- `reports/publication/spark-treatment-table.tex`: LaTeX treatment result table.
- `reports/figures/observed-spark-baseline-proposed.svg`: two-dataset Spark comparison figure.
- `reports/figures/iccit-smartphone-str-20260527T073310Z-ablation.svg`: smartphone ablation figure.
- `reports/figures/iccit-road-full-20q-20260527T094500Z-ablation.svg`: road ablation figure.

## Paper Sources

- `papers/An_Efficient_Distributed_Framework_for_Top-k_Dominating_Query_Over_Uncertain_Databases.pdf`, Sections V-A through V-D, Tables II-III.
- `papers/s10115-023-01917-3(Target Paper).pdf`, Sections 5 and 7, Table 2 and Figures 7-13.
