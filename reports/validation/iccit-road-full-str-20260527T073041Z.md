# Same-Machine ICCIT/Spark Comparison: `iccit-road-full-str-20260527T073041Z`

Generated: 2026-05-27T07:38:28.317091+00:00

## Interpretation Boundary

This report compares Spark treatment variants against each other on the current machine. It does
not compare absolute Spark milliseconds to the published Hadoop milliseconds because hardware,
engine, data curation and undisclosed controls differ. The ICCIT paper does not report `k`,
partition count or query seeds; this execution records assumed values explicitly.

| Setting | Value |
|---|---|
| Profile | `road-full` |
| k (declared assumption) | `10` |
| partitions (declared assumption) | `8` |
| Exact oracle during performance run | `false` |
| Spark driver memory | `8g` |
| Spark master | `local[4]` |
| Bound mode | `rai-lian-artree-selected-level-partial-reducer` |

## Published ICCIT Reference Only

| Paper dataset | Baseline Hadoop act WC | AES+DSCP Hadoop act WC | Reduction |
|---|---:|---:|---:|
| road-full reference | 56,274 ms | 42,366 ms | 24.7% |

## Observed Spark Same-Machine Treatments

| Treatment | Algorithm ms | Reduction vs Spark baseline | Candidate filtered | Emitted records | Shuffle bytes | Partial MBR refs | Exact agreement |
|---|---:|---:|---:|---:|---:|---:|---|
| baseline | 18,034 | 0.00% | 99.30% | 1,432,467 | 93,168,331 | 1,432,467 | not run |
| aes-only | 13,728 | 23.88% | 99.30% | 44,400 | 76,746,071 | 1,432,467 | not run |
| dscp-only | 14,493 | 19.64% | 99.52% | 914,440 | 86,085,003 | 914,440 | not run |
| aes-dscp | 12,062 | 33.12% | 99.52% | 30,152 | 75,654,443 | 914,440 | not run |

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
