# Same-Machine ICCIT/Spark Comparison: `artree-road-smoke-traversalcost-20260527T034756Z-652436`

Generated: 2026-05-27T03:48:30.508439+00:00

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
| Spark driver memory | `2g` |
| Bound mode | `rai-lian-artree-selected-level-partial-reducer` |

## Published ICCIT Reference Only

| Paper dataset | Baseline Hadoop act WC | AES+DSCP Hadoop act WC | Reduction |
|---|---:|---:|---:|
| road-smoke reference | 56,274 ms | 42,366 ms | 24.7% |

## Observed Spark Same-Machine Treatments

| Treatment | Algorithm ms | Reduction vs Spark baseline | Pruned | Emitted records | Shuffle bytes | Partial MBR refs | Exact agreement |
|---|---:|---:|---:|---:|---:|---:|---|
| baseline | 3,605 | 0.00% | n/a | 850 | 51,771 | 850 | True |
| aes-only | 3,932 | -9.07% | n/a | 850 | 51,771 | 850 | True |
| dscp-only | 2,800 | 22.33% | 2.50% | 850 | 52,349 | 850 | True |
| aes-dscp | 4,489 | -24.52% | 2.50% | 850 | 52,349 | 850 | True |

For paper-sized performance runs with exact validation disabled, exactness evidence must be
paired with the validated deterministic MBR suite and road smoke suite before results are used in
a manuscript. This runtime implements a packed aggregate R-tree per partition, selected exported
index levels and reducer-stage traversal of partial MBR references. The selected-level estimate
uses a deterministic representative candidate-instance probe sample and estimated reducer
subtree traversal work as a stand-in for the historical/uniform query calibration described in
the target paper; this is a declared Spark adaptation of Rai and Lian's index distribution
policy.
