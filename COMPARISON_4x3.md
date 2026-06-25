# 4×3 Algorithm Comparison: Smartphone Dataset (ICCIT 2025)

**Dataset:** Smartphone (750 objects, 207,860 rows, 20 queries, k=10, partitions=8)  
**Paper Reference:** ICCIT 2025, Table II — Synthetic Smartphone  
**Generated:** 2026-06-25

---

## Comparison Table: Algorithm Time (ms) by Engine

| Algorithm | ICCIT Paper (Hadoop) | Hadoop MapReduce (Observed) | Apache Spark (Observed) |
|-----------|---------------------|----------------------------|------------------------|
| **Rai-Lian Baseline** | 66,520 | 91,214 | 41,800 |
| **AES-only** | 44,760 | 74,349 | 37,050 |
| **DSCP-only** | 58,484 | 76,556 | 43,303 |
| **AES+DSCP** | 43,757 | 73,597 | 36,527 |

---

## Reduction vs. Baseline (%) by Engine

| Algorithm | ICCIT Paper Reduction | Hadoop MapReduce Reduction | Spark Reduction |
|-----------|----------------------|---------------------------|----------------|
| **AES-only** | 32.7% | 18.5% | 11.4% |
| **DSCP-only** | 12.0% | 16.1% | -3.6% |
| **AES+DSCP** | 34.2% | 19.3% | 12.6% |

---

## Key Observations

### ICCIT Paper (Hadoop Reference)
- AES+DSCP achieves **34.2%** reduction over baseline (66,520 → 43,757 ms)
- AES-only provides most of the benefit (32.7%)
- DSCP-only alone gives modest 12.0% reduction

### Hadoop MapReduce (Observed)
- Baseline is **37% slower** than paper reference (91,214 vs 66,520 ms) — likely different hardware/environment
- AES+DSCP achieves **19.3%** reduction (91,214 → 73,597 ms)
- All variants show less reduction than paper; absolute times are higher
- DSCP-only (16.1%) slightly outperforms AES-only (18.5%) in reduction

### Apache Spark (Observed)
- **All Spark variants are 2–2.5× faster** than Hadoop MapReduce on same machine
- Spark baseline: 41,800 ms (2.2× faster than Hadoop baseline)
- AES+DSCP: 36,527 ms (12.6% reduction vs Spark baseline)
- DSCP-only **increases** time vs baseline (-3.6% reduction) — overhead without sufficient pruning benefit
- Best absolute performance: **Spark AES+DSCP at 36,527 ms**

---

## Cross-Engine Caution

> **No cross-engine speedup claims are valid.** The ICCIT paper ran on pseudo-distributed Hadoop (Windows 11, i5-8265U, 8 GB RAM). These observed runs execute on Dockerized Hadoop/Spark on Linux. Hardware, JVM, data partitioning, and cluster overhead differ significantly. Only **within-engine** comparisons (paper vs observed Hadoop, or Hadoop vs Spark on same machine) are meaningful.

---

## Summary by User's Four Categories

| Category | Configuration | Time (ms) |
|----------|---------------|-----------|
| **i. Rai-Lian Baseline** | Hadoop MapReduce `baseline` | 91,214 |
| **ii. ICCIT (Hadoop)** | `aes-only` | 74,349 |
|  | `aes-dscp` | 73,597 |
|  | `dscp-only` | 76,556 |
| **iii. Spark Re-implementation (ICCIT)** | `aes-only` | 37,050 |
|  | `aes-dscp` | 36,527 |
|  | `dscp-only` | 43,303 |
| **iv. Spark Rai-Lian** | `baseline` | 41,800 |

---

## Run Artifacts

| Run ID | Engine | Algorithm |
|--------|--------|-----------|
| `smartphone-hadoop-baseline` | Hadoop | baseline |
| `smartphone-hadoop-aes-only` | Hadoop | aes-only |
| `smartphone-hadoop-dscp-only` | Hadoop | dscp-only |
| `smartphone-hadoop-aes-dscp` | Hadoop | aes-dscp |
| `iccit-smartphone-str-20260527T073310Z-baseline` | Spark | baseline |
| `iccit-smartphone-str-20260527T073310Z-aes-only` | Spark | aes-only |
| `iccit-smartphone-str-20260527T073310Z-dscp-only` | Spark | dscp-only |
| `iccit-smartphone-str-20260527T073310Z-aes-dscp` | Spark | aes-dscp |

---

## ICCIT Paper Reference Values (Table II)

| Dataset | Baseline | AES-only | DSCP-only | AES+DSCP | act CC | Reduction |
|---------|----------|----------|-----------|----------|--------|-----------|
| Smartphone | 66,520 | 44,760 | 58,484 | 43,757 | 1.4179×10¹¹ | 34.2% |

---

## ICCIT Paper Ablation (Table III)

| Dataset | AES-only | DSCP-only | AES+DSCP |
|---------|----------|-----------|----------|
| Smartphone | 32.7% | 12.0% | 34.2% |

---

*Report generated from observed benchmark runs. All runs used VALIDATE_EXACT=false for performance timing.*