package com.thesis.topk.simulator;

public record SimulationConfig(
    int objects,
    int dimensions,
    int queries,
    int k,
    double missingRate,
    long seed,
    long startTimeMs,
    long eventGapMs) {

  public static SimulationConfig defaults() {
    return new SimulationConfig(500, 4, 2, 10, 0.2, 42L, 1_700_000_000_000L, 20L);
  }
}

