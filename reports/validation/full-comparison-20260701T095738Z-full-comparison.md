# Full Comparison Suite: `full-comparison-20260701T095738Z`

Generated: 2026-07-01T10:09:40.745704+00:00

## Configuration

| Setting | Value |
|---|---|
| Profile | `smartphone` / Synthetic smartphone |
| k | 10 |
| partitions | 8 |
| Exact validation during run | false |
| Spark driver memory | 2g |
| Spark master | local[4] |

## Published ICCIT Paper (Hadoop MapReduce) Reference

These reference values are from the ICCIT 2025 paper. Hardware and runtime conditions differ from the observed runs below. Assumptions for `k`, partitions, and query seeds are declared protocol values. The paper uses 20 random queries averaged (Table II, III).

**Table II — Improvement of Proposed Algorithm (act WC):**

| Dataset | Baseline (ms) | AES-only (ms) | DSCP-only (ms) | AES+DSCP (ms) | act CC | Reduction |
|---|---|---|---|---|---|---|
| Synthetic smartphone | 66,520 | 44,760 | 58,484 | 43,757 | 1.4179×10^11 | 34.2% |

**Table III — Ablation: Wall Clock Time Reduction vs Baseline (%):**

| Dataset | AES-only | DSCP-only | AES+DSCP |
|---|---|---|---|
| Synthetic smartphone | 32.7% | 12.0% | 34.2% |

## Observed Results

### Per-Variant Comparison

| Engine | Variant | Algorithm ms | Prune ratio | Emitted records | AER | Shuffle bytes | Exact agreement |
|---|---|---|---|---|---|---|---|
| hadoop | baseline | 63,334 ms | 0.00% | 19,340,960 | 0.010747 | 1,856,732,160 | not run |
| hadoop | dscp-only | 64,676 ms | 0.00% | 19,340,960 | 0.010747 | 1,856,732,160 | not run |
| hadoop | aes-only | 65,389 ms | 0.00% | 207,860 | 0.010747 | 19,954,560 | not run |
| hadoop | aes-dscp | 65,984 ms | 0.00% | 207,860 | 0.010747 | 19,954,560 | not run |
| spark | baseline | 45,307 ms | 0.01% | 6,358,738 | 0.26389835 | 151,541,820 | not run |
| spark | dscp-only | 19,246 ms | 88.50% | 925,854 | 0.18823004999999998 | 38,031,972 | not run |
| spark | aes-only | 34,748 ms | 0.01% | 1,659,738 | 0.26389835 | 104,931,788 | not run |
| spark | aes-dscp | 17,121 ms | 88.50% | 171,856 | 0.18823004999999998 | 30,304,632 | not run |

### Within-Engine Reduction vs Baseline

| Engine | Baseline (ms) | AES-only (ms) | DSCP-only (ms) | AES+DSCP (ms) | Reduction (AES+DSCP vs Baseline) |
|---|---|---|---|---|---|---|

## Interpretation

- **Rai-Lian baseline** = `baseline` variant: aR-tree score-bound baseline without AES or DSCP extensions
- **ICCTT Hadoop** = all 4 variants run on Hadoop MapReduce engine
- **Spark** = the same 4 treatment variants run on Apache Spark RDD engine
- The Rai-Lian baseline and ICCIT variants share the same `PtdAlgorithm` definitions: the only difference is the execution engine (Hadoop vs Spark)
- Published Hadoop ms are reference only — no cross-engine speedup claim is valid given differing hardware

**Validation result: All checks passed**
