# Full Comparison Suite: `full-comparison-20260701T094413Z`

Generated: 2026-07-01T09:50:13.631688+00:00

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
| hadoop | baseline | 81 ms | 0.00% | 15,119 | 0.06601 | 1,451,424 | not run |
| hadoop | dscp-only | 88 ms | 0.00% | 15,119 | 0.06601 | 1,451,424 | not run |
| hadoop | aes-only | 116 ms | 0.00% | 998 | 0.06601 | 95,808 | not run |
| hadoop | aes-dscp | 82 ms | 0.00% | 998 | 0.06601 | 95,808 | not run |
| spark | baseline | 4,322 ms | 0.00% | 9,807 | 0.79448 | 541,454 | not run |
| spark | dscp-only | 2,355 ms | 0.00% | 9,807 | 0.79448 | 543,090 | not run |
| spark | aes-only | 2,766 ms | 0.00% | 7,705 | 0.79448 | 520,446 | not run |
| spark | aes-dscp | 2,664 ms | 0.00% | 7,705 | 0.79448 | 522,082 | not run |

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
