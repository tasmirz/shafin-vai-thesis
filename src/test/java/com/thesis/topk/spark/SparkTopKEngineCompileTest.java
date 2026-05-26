package com.thesis.topk.spark;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class SparkTopKEngineCompileTest {
  @Test
  void sparkEngineResultTypesAreAvailableToMavenTests() {
    SparkTopKEngine.QueryRanking ranking =
        new SparkTopKEngine.QueryRanking("q0", java.util.List.of(), 10, 7, 3, 0.2, 10, true, true, 0L);
    assertThat(ranking.pruneRatio()).isEqualTo(0.3);
    assertThat(ranking.validationPerformed()).isTrue();
    assertThat(ranking.exactAgreement()).isTrue();
    assertThat(ranking.algorithmId()).isEqualTo("aes-dscp");
    assertThat(ranking.aggregatedEmissionRate()).isEqualTo(1.0);
  }
}
