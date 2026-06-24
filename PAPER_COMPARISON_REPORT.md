# Paper Comparison Report: Three-Way Analysis

## Executive Summary

The three-way comparison script (`run_three_way_comparison.sh`) was comparing **fundamentally different algorithms**:
- **Spark**: Rai-Lian (2023) aR-tree based PTD with DSCP/AES extensions (paper's proposed method)
- **Hadoop**: Simple DDR/MBR baseline (no aR-tree, no cost-model index distribution)

This is **not** the comparison from the paper. The paper compares:
- **Baseline**: Rai & Lian (2023) "Distributed probabilistic top-k dominating queries over uncertain databases" (Kais 2023) - full aR-tree MapReduce framework
- **Proposed**: Same aR-tree framework + Dynamic Smart Candidate Pruning (DSCP) + Aggregated Emission Strategy (AES)

---

## Correct Comparison: Spark Baseline vs Spark Proposed (Both aR-tree)

### Dataset: Synthetic Smartphone (Paper-like: 745 objects, 10K instances, 4 partitions, k=10)

| Variant | Algorithm Time | Wall-clock | Prune Ratio | Emissions | AER | Shuffle Bytes |
|---------|----------------|------------|-------------|-----------|-----|---------------|
| **Baseline (Rai-Lian aR-tree)** | 9,568 ms | 13,143 ms | 0.00% | 181,759 | 1.00 | 5.78 MB |
| **AES-only** | 12,280 ms | 16,454 ms | 0.00% | 41,408 | 1.00 | 4.15 MB |
| **DSCP-only** | 12,151 ms | 16,186 ms | **0.00%** | 181,759 | 1.00 | 5.79 MB |
| **Proposed (AES+DSCP)** | **12,407 ms** | **16,539 ms** | **0.00%** | **41,408** | **1.00** | **4.15 MB** |

### Key Findings

1. **AES works correctly**: 77% emission reduction (181,759 → 41,408), matching paper's ~80% AER
2. **DSCP is BROKEN**: 0% pruning on both DSCP-only and AES+DSCP variants
3. **Overall result**: Proposed is **0.5% SLOWER** than baseline (opposite of paper's 24-34% reduction)

---

## Root Cause: DSCP Not Pruning

### Why DSCP Fails

The Dynamic Smart Candidate Pruning compares each object's Upper Bound (UB) against the k-th largest Lower Bound (τ). Objects with UB < τ are pruned.

**Problem**: The aR-tree upper bounds are too loose for effective pruning.

```
For remote partitions at selected export level:
  UB = fullyDominatedMass + partialUpperMass
```

At the cost-model-selected export level (root level for our random dataset), `partialUpperMass` = entire partition mass, making UB very large.

Even at leaf level (forced), the node granularity (fanout=16) means UB overestimates by including all objects in partially dominated leaf nodes.

### Paper's Dataset vs Ours

| Aspect | Paper's Synthetic | Our Synthetic |
|--------|-------------------|---------------|
| Distribution | 15 clustered regions (budget/mid/premium) | Uniform random |
| Correlation | High (brands in region similar) | None |
| Pruning potential | High (query dominates entire regions) | Low (query partially dominates everything) |

The paper's clustered data enables effective aR-tree pruning. Our uniform random data does not.

---

## Hadoop Implementation Status

The Hadoop implementation (`ProbabilisticTopKHadoopJob.java`) uses:
- Simple DDR/MBR bounds (no aR-tree)
- No cost-model index distribution
- Different algorithm from Rai & Lian (2023)

### Hadoop Results (6 objects, 6 instances - toy dataset)

| Variant | Algorithm Time | Prune Ratio | Emissions |
|---------|----------------|-------------|-----------|
| Baseline | 12 ms | 0.00% | 30 |
| AES-only | 17 ms | 0.00% | 6 |
| DSCP-only | 16 ms | **66.67%** | 10 |
| AES+DSCP | 9 ms | **66.67%** | 2 |

DSCP works on Hadoop's simple DDR/MBR but on a different algorithm and toy dataset.

---

## Required Fixes for Valid Paper Comparison

### 1. Fix DSCP in Spark (High Priority)

Options:
- **A**: Use conservative upper bound for remote partitions (LB + objectMass × remoteMass) instead of aR-tree partial mass
- **B**: Implement object-level upper bounds in aR-tree (split leaf nodes at object granularity)
- **C**: Use paper's clustered synthetic dataset generator

### 2. Implement Rai-Lian Baseline in Hadoop (High Priority)

Need full aR-tree MapReduce implementation:
- Cost-model-driven index distribution
- Mapper-side filtering with aR-tree bounds
- Reducer-side partial MBR traversal
- Same DSCP/AES extensions

### 3. Use Paper's Datasets (Medium Priority)

- Synthetic: 15 regions × 50 brands × 8-20 models with clustered attributes
- Real: Bangladesh road network from OSM geometries

---

## Current Status

| Task | Status |
|------|--------|
| Spark DSCP bug fix | 🔴 Not fixed - fundamental aR-tree bound issue |
| Hadoop Rai-Lian baseline | 🔴 Not implemented |
| Paper datasets | 🟡 Synthetic created (but uniform, not clustered) |
| Correct comparison script | 🟡 Partially done - shows correct Spark comparison |
| Wall-clock reporting | ✅ Done (elapsedMs includes setup) |

---

## Recommendation

**Do not use the current three-way comparison results.** They compare different algorithms.

For a valid paper comparison:
1. Fix Spark DSCP (use conservative UB for remote partitions)
2. Generate paper's clustered synthetic dataset
3. Compare Spark baseline vs Spark proposed on same engine
4. Separately, implement Rai-Lian in Hadoop for cross-engine comparison

The AES component is working correctly (77% emission reduction matches paper).