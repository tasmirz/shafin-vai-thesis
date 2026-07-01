# 4×3 Algorithm Comparison: Smartphone Dataset (ICCIT 2025)

**Dataset:** Smartphone (750 objects, 207,860 rows, 20 queries, k=10, partitions=8)  
**Paper Reference:** ICCIT 2025, Table II — Synthetic Smartphone  
**Generated:** 2026-07-01

---

## Comparison Table: Algorithm Time (ms) by Engine

| Algorithm | ICCIT Paper (Hadoop) | Hadoop MapReduce (Observed) | Apache Spark (Observed) |
|-----------|---------------------|----------------------------|------------------------|
| **Rai-Lian Baseline** | 66,520 | 63,334 | 45,307 |
| **AES-only** | 44,760 | 65,389 | 34,748 |
| **DSCP-only** | 58,484 | 64,676 | 19,246 |
| **AES+DSCP** | 43,757 | 65,984 | 17,121 |

---

## Reduction vs. Baseline (%) by Engine

| Algorithm | ICCIT Paper Reduction | Hadoop MapReduce Reduction | Spark Reduction |
|-----------|----------------------|---------------------------|----------------|
| **AES-only** | 32.7% | -3.24% | 23.31% |
| **DSCP-only** | 12.0% | -2.12% | 57.52% |
| **AES+DSCP** | 34.2% | -4.18% | 62.21% |

---

## Key Observations

### ICCIT Paper (Hadoop Reference)
- AES+DSCP achieves **34.2%** reduction over baseline (66,520 → 43,757 ms)
- AES-only provides most of the benefit (32.7%)
- DSCP-only alone gives modest 12.0% reduction

### Hadoop MapReduce (Observed)
- Baseline is 63,334 ms (slightly faster than paper reference 66,520 ms)
- Optimizations showed negative reduction (AES+DSCP is **-4.18%** slower than baseline)
- The high absolute overhead of local map-reduce containers at this scale obscures algorithmic gains

### Apache Spark (Observed)
- Spark baseline: 45,307 ms
- AES+DSCP achieves **62.21%** reduction vs Spark baseline (45,307 → 17,121 ms)
- DSCP-only achieves **57.52%** reduction vs Spark baseline (45,307 → 19,246 ms)
- Best absolute performance: **Spark AES+DSCP at 17,121 ms**

---

## Cross-Engine Caution

> **No cross-engine speedup claims are valid.** The ICCIT paper ran on pseudo-distributed Hadoop (Windows 11, i5-8265U, 8 GB RAM). These observed runs execute on Dockerized Hadoop/Spark on Linux. Hardware, JVM, data partitioning, and cluster overhead differ significantly. Only **within-engine** comparisons (paper vs observed Hadoop, or Hadoop vs Spark on same machine) are meaningful.

---

## Summary by User's Four Categories

| Category | Configuration | Time (ms) |
|----------|---------------|-----------|
| **i. Rai-Lian Baseline** | Hadoop MapReduce `baseline` | 63,334 |
| **ii. ICCIT (Hadoop)** | `aes-only` | 65,389 |
|  | `aes-dscp` | 65,984 |
|  | `dscp-only` | 64,676 |
| **iii. Spark Re-implementation (ICCIT)** | `aes-only` | 34,748 |
|  | `aes-dscp` | 17,121 |
|  | `dscp-only` | 19,246 |
| **iv. Spark Rai-Lian** | `baseline` | 45,307 |

---

## Run Artifacts

| Run ID | Engine | Algorithm |
|--------|--------|-----------|
| `full-comparison-20260701T095738Z` | Hadoop | baseline |
| `full-comparison-20260701T095738Z` | Hadoop | aes-only |
| `full-comparison-20260701T095738Z` | Hadoop | dscp-only |
| `full-comparison-20260701T095738Z` | Hadoop | aes-dscp |
| `full-comparison-20260701T095738Z` | Spark | baseline |
| `full-comparison-20260701T095738Z` | Spark | aes-only |
| `full-comparison-20260701T095738Z` | Spark | dscp-only |
| `full-comparison-20260701T095738Z` | Spark | aes-dscp |

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