# Same-Machine ICCIT/Spark Comparison: `iccit-smartphone-full-traversalcost-20260527T034934Z-655709`

Generated: 2026-05-27T04:01:16.100268+00:00

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
| Spark driver memory | `2g` |
| Bound mode | `rai-lian-artree-selected-level-partial-reducer` |

## Published ICCIT Reference Only

| Paper dataset | Baseline Hadoop act WC | AES+DSCP Hadoop act WC | Reduction |
|---|---:|---:|---:|
| smartphone reference | 66,520 ms | 43,757 ms | 34.2% |

## Observed Spark Same-Machine Treatments

| Treatment | Algorithm ms | Reduction vs Spark baseline | Pruned | Emitted records | Shuffle bytes | Partial MBR refs | Exact agreement |
|---|---:|---:|---:|---:|---:|---:|---|
| baseline | 113,241 | 0.00% | n/a | 6,311,894 | 133,738,783 | 6,311,894 | not run |
| aes-only | 105,743 | 6.62% | n/a | 1,450,120 | 85,480,464 | 6,311,894 | not run |
| dscp-only | 143,299 | -26.54% | 0.00% | 6,311,894 | 133,891,409 | 6,311,894 | not run |
| aes-dscp | 331,345 | -192.60% | 0.00% | 1,450,120 | 85,633,090 | 6,311,894 | not run |

For paper-sized performance runs with exact validation disabled, exactness evidence must be
paired with the validated deterministic MBR suite and road smoke suite before results are used in
a manuscript. This runtime implements a packed aggregate R-tree per partition, selected exported
index levels and reducer-stage traversal of partial MBR references. The selected-level estimate
uses a deterministic representative candidate-instance probe sample and estimated reducer
subtree traversal work as a stand-in for the historical/uniform query calibration described in
the target paper; this is a declared Spark adaptation of Rai and Lian's index distribution
policy.
