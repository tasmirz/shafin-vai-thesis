package com.thesis.topk.spark;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class SparkTopKEngineCompileTest {
  @Test
  void sparkEngineResultTypesAreAvailableToMavenTests() {
    SparkTopKEngine.QueryRanking ranking = new SparkTopKEngine.QueryRanking("q0", java.util.List.of(), 10, 7, 3, 0.2, 10);
    assertThat(ranking.pruneRatio()).isEqualTo(0.3);
  }
}
