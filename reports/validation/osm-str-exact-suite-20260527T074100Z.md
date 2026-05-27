# Same-Machine ICCIT/Spark Comparison: `osm-str-exact-suite-20260527T074100Z`

Generated: 2026-05-27T07:40:55.008349+00:00

## Interpretation Boundary

This report compares Spark treatment variants against each other on the current machine. It does
not compare absolute Spark milliseconds to the published Hadoop milliseconds because hardware,
engine, data curation and undisclosed controls differ. The ICCIT paper does not report `k`,
partition count or query seeds; this execution records assumed values explicitly.

| Setting | Value |
|---|---|
| Profile | `road-smoke` |
| k (declared assumption) | `5` |
| partitions (declared assumption) | `4` |
| Exact oracle during performance run | `true` |
| Spark driver memory | `4g` |
| Spark master | `local[4]` |
| Bound mode | `rai-lian-artree-selected-level-partial-reducer` |

## Published ICCIT Reference Only

| Paper dataset | Baseline Hadoop act WC | AES+DSCP Hadoop act WC | Reduction |
|---|---:|---:|---:|
| road-smoke reference | 56,274 ms | 42,366 ms | 24.7% |

## Observed Spark Same-Machine Treatments

| Treatment | Algorithm ms | Reduction vs Spark baseline | Candidate filtered | Emitted records | Shuffle bytes | Partial MBR refs | Exact agreement |
|---|---:|---:|---:|---:|---:|---:|---|
| baseline | 986 | 0.00% | 0.00% | 1,160 | 78,179 | 1,160 | True |
| aes-only | 1,138 | -15.42% | 0.00% | 1,160 | 78,179 | 1,160 | True |
| dscp-only | 871 | 11.66% | 0.00% | 1,160 | 78,429 | 1,160 | True |
| aes-dscp | 1,197 | -21.40% | 0.00% | 1,160 | 78,429 | 1,160 | True |

For paper-sized performance runs with exact validation disabled, exactness evidence must be
paired with the validated deterministic MBR suite and road smoke suite before results are used in
a manuscript. `Candidate filtered` includes the Rai-Lian indexed baseline's bound filtering;
the DSCP contribution is the additional filtering and runtime difference relative to that indexed
baseline, not the full value in that column. This runtime implements an STR-packed aggregate
R-tree per partition, selected exported
index levels and reducer-stage traversal of partial MBR references. The selected-level estimate
uses a deterministic representative candidate-instance probe sample and estimated reducer
subtree traversal work as a stand-in for the historical/uniform query calibration described in
the target paper; this is a declared Spark adaptation of Rai and Lian's index distribution
policy.
