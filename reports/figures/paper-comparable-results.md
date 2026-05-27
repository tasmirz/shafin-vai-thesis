# Paper-Style Comparable Results

Generated: 2026-05-27T07:37:19.742691+00:00

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
| Bangladesh road | `iccit-road-full-str-20260527T073041Z` | 18,034 | 23.88% | 19.64% | 33.12% |

## Figure Artifacts

- `observed-spark-baseline-proposed.svg`: observed baseline/proposed comparison.
- `iccit-smartphone-str-20260527T073310Z-ablation.svg`: observed four-treatment ablation chart.
- `iccit-road-full-str-20260527T073041Z-ablation.svg`: observed four-treatment ablation chart.
