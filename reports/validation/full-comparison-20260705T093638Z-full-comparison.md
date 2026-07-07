# Full Comparison Suite: `full-comparison-20260705T093638Z`

Generated: 2026-07-05T09:37:58.980112+00:00

## Configuration

| Setting | Value |
|---|---|
| Profile | `road-smoke` / Bangladesh road / OSM (smoke) |
| k | 10 |
| partitions | 8 |
| Exact validation during run | true |
| Spark driver memory | 2g |
| Spark master | local[4] |

## Published ICCIT Paper (Hadoop MapReduce) Reference

These reference values are from the ICCIT 2025 paper. Hardware and runtime conditions differ from the observed runs below. Assumptions for `k`, partitions, and query seeds are declared protocol values. The paper uses 20 random queries averaged (Table II, III).

**Table II — Improvement of Proposed Algorithm (act WC):**

| Dataset | Baseline (ms) | AES-only (ms) | DSCP-only (ms) | AES+DSCP (ms) | act CC | Reduction |
|---|---|---|---|---|---|---|
| Bangladesh road / OSM (smoke) | 56,274 | 42,967 | 46,848 | 42,366 | 1.54×10^10 | 24.7% |

**Table III — Ablation: Wall Clock Time Reduction vs Baseline (%):**

| Dataset | AES-only | DSCP-only | AES+DSCP |
|---|---|---|---|
| Bangladesh road / OSM (smoke) | 23.6% | 16.8% | 24.7% |

## Observed Results

### Per-Variant Comparison

| Engine | Variant | Algorithm ms | Prune ratio | Emitted records | AER | Shuffle bytes | Exact agreement |
|---|---|---|---|---|---|---|---|
| spark | baseline | 919 ms | 0.00% | 850 | 1.0 | 74,656 | True |
| spark | dscp-only | 1,079 ms | 10.00% | 754 | 1.0 | 71,551 | True |
| spark | aes-only | 1,038 ms | 0.00% | 850 | 1.0 | 74,656 | True |
| spark | aes-dscp | 2,293 ms | 10.00% | 754 | 1.0 | 71,551 | True |

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
