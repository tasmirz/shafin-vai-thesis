# Same-Machine ICCIT/Spark Comparison: `iccit-smartphone-str-20260527T073310Z`

Generated: 2026-05-27T07:38:28.333313+00:00

## Interpretation Boundary

This report compares Spark treatment variants against each other on the current machine. It does
not compare absolute Spark milliseconds to the published Hadoop milliseconds because hardware,
engine, data curation and undisclosed controls differ. The ICCIT paper does not report `k`,
partition count or query seeds; this execution records assumed values explicitly.

| Setting | Value |
|---|---|
| Profile | `smartphone` |
| k (declared assumption) | `10` |
| partitions (declared assumption) | `8` |
| Exact oracle during performance run | `false` |
| Spark driver memory | `8g` |
| Spark master | `local[4]` |
| Bound mode | `rai-lian-artree-selected-level-partial-reducer` |

## Published ICCIT Reference Only

| Paper dataset | Baseline Hadoop act WC | AES+DSCP Hadoop act WC | Reduction |
|---|---:|---:|---:|
| smartphone reference | 66,520 ms | 43,757 ms | 34.2% |

## Observed Spark Same-Machine Treatments

| Treatment | Algorithm ms | Reduction vs Spark baseline | Candidate filtered | Emitted records | Shuffle bytes | Partial MBR refs | Exact agreement |
|---|---:|---:|---:|---:|---:|---:|---|
| baseline | 41,800 | 0.00% | 0.00% | 6,358,938 | 151,550,746 | 6,358,938 | not run |
| aes-only | 37,050 | 11.36% | 0.00% | 1,659,913 | 104,940,242 | 6,358,938 | not run |
| dscp-only | 43,303 | -3.60% | 3.05% | 6,302,572 | 149,227,453 | 6,302,572 | not run |
| aes-dscp | 36,527 | 12.61% | 3.05% | 1,609,878 | 102,649,060 | 6,302,572 | not run |

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
