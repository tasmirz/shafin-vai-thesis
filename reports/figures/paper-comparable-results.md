# Paper-Style Comparable Results

Generated: 2026-05-27T10:29:03.365993+00:00

Published Hadoop values and observed Spark values are shown separately. Absolute wall-clock
numbers cannot be compared as algorithmic speedups across different hardware/runtime setups.

## Published ICCIT Hadoop Reference

| Dataset | Baseline ms | AES-only ms | DSCP-only ms | AES+DSCP ms | Full reduction |
|---|---:|---:|---:|---:|---:|
| Synthetic | 66,520 | 44,760 | 58,484 | 43,757 | 34.2% |
| Real | 56,274 | 42,967 | 46,848 | 42,366 | 24.7% |

## Observed Spark Treatment Matrix

| Dataset | Suite | Baseline ms | AES-only reduction | DSCP-only reduction | AES+DSCP reduction |
|---|---|---:|---:|---:|---:|
| Synthetic smartphone | `iccit-smartphone-str-20260527T073310Z` | 41,800 | 11.36% | -3.60% | 12.61% |
| Bangladesh road | `iccit-road-full-20q-20260527T094500Z` | 917,176 | 43.41% | 34.22% | 58.61% |

## ICCIT Reference, Spark Baseline, And Spark Upgrade

The `Spark indexed baseline` column is the implemented Rai-Lian-style distributed aR-tree
treatment executed through Spark without the ICCIT AES/DSCP extensions. The published ICCIT
figures were measured under Hadoop on different hardware and are reference values, not a
same-machine engine speed comparison.

| Dataset | Published ICCIT Hadoop baseline | Published ICCIT Hadoop AES+DSCP | Spark indexed baseline | Spark AES+DSCP upgrade | Within-Spark reduction |
|---|---:|---:|---:|---:|---:|
| Synthetic smartphone | 66,520 ms | 43,757 ms | 41,800 ms | 36,527 ms | 12.61% |
| Bangladesh road | 56,274 ms | 42,366 ms | 917,176 ms | 379,637 ms | 58.61% |

## Figure Artifacts

- `observed-spark-baseline-proposed.svg`: observed baseline/proposed comparison.
- `iccit-smartphone-str-20260527T073310Z-ablation.svg`: observed four-treatment ablation chart.
- `iccit-road-full-20q-20260527T094500Z-ablation.svg`: observed four-treatment ablation chart.
