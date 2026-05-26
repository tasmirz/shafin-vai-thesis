# Same-Machine ICCIT/Spark Comparison: `iccit-smartphone-full-indexed-20260526T222945Z-596055`

Generated: 2026-05-26T22:37:53.746905+00:00

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
| Bound mode | `rai-lian-artree-selected-level-partial-reducer` |

## Published ICCIT Reference Only

| Paper dataset | Baseline Hadoop act WC | AES+DSCP Hadoop act WC | Reduction |
|---|---:|---:|---:|
| smartphone reference | 66,520 ms | 43,757 ms | 34.2% |

## Observed Spark Same-Machine Treatments

| Treatment | Algorithm ms | Reduction vs Spark baseline | Pruned | Emitted records | Shuffle bytes | Partial MBR refs | Exact agreement |
|---|---:|---:|---:|---:|---:|---:|---|
| baseline | 93,400 | 0.00% | n/a | 1,453,334 | 79,313,646 | 1,453,334 | not run |
| aes-only | 100,922 | -8.05% | n/a | 1,453,334 | 79,313,646 | 1,453,334 | not run |
| dscp-only | 114,884 | -23.00% | 0.00% | 1,453,334 | 79,466,080 | 1,453,334 | not run |
| aes-dscp | 126,257 | -35.18% | 0.00% | 1,453,334 | 79,466,080 | 1,453,334 | not run |

For paper-sized performance runs with exact validation disabled, exactness evidence must be
paired with the validated deterministic MBR suite and road smoke suite before results are used in
a manuscript. This runtime implements a packed aggregate R-tree per partition, selected exported
index levels and reducer-stage traversal of partial MBR references. The selected-level estimate
uses the finite configured query workload as the query-log sample; this is a declared Spark
adaptation of Rai and Lian's index distribution policy.
