package com.thesis.topk.algorithm;

import static org.assertj.core.api.Assertions.assertThat;

import com.thesis.topk.model.ProbabilisticInstance;
import com.thesis.topk.model.QueryPoint;
import java.util.List;
import org.junit.jupiter.api.Test;

class DominanceScorerTest {
  @Test
  void detectsDynamicDominanceRelativeToQueryPoint() {
    QueryPoint query = new QueryPoint("q0", new double[] {0.0, 0.0});
    ProbabilisticInstance near = instance("near", 1.0, 0.1, 0.2);
    ProbabilisticInstance far = instance("far", 1.0, 0.3, 0.2);

    assertThat(DominanceScorer.dynamicallyDominates(near, far, query)).isTrue();
    assertThat(DominanceScorer.dynamicallyDominates(far, near, query)).isFalse();
  }

  @Test
  void computesExpectedDominanceMass() {
    QueryPoint query = new QueryPoint("q0", new double[] {0.0, 0.0});
    ProbabilisticInstance a = instance("a", 1.0, 0.1, 0.1);
    ProbabilisticInstance b = instance("b", 0.4, 0.2, 0.2);
    ProbabilisticInstance c = instance("c", 0.6, 0.3, 0.3);

    double score = DominanceScorer.expectedDominanceScore(List.of(a), List.of(a, b, c), query);

    assertThat(score).isCloseTo(1.0, org.assertj.core.data.Offset.offset(1.0e-9));
  }

  @Test
  void computesSafeFullAndPossibleDynamicDominanceAgainstMbr() {
    QueryPoint query = new QueryPoint("q0", new double[] {0.0, 0.0});
    ProbabilisticInstance near = instance("near", 1.0, 0.1, 0.1);
    ProbabilisticInstance overlapping = instance("overlap", 1.0, 0.25, 0.25);

    assertThat(DominanceScorer.dynamicallyDominatesMbrFully(
        near, new double[] {0.2, 0.2}, new double[] {0.4, 0.4}, query)).isTrue();
    assertThat(DominanceScorer.dynamicallyDominatesMbrFully(
        overlapping, new double[] {0.2, 0.2}, new double[] {0.4, 0.4}, query)).isFalse();
    assertThat(DominanceScorer.dynamicallyDominatesMbrPossibly(
        overlapping, new double[] {0.2, 0.2}, new double[] {0.4, 0.4}, query)).isTrue();
  }

  private static ProbabilisticInstance instance(String id, double probability, double x, double y) {
    return new ProbabilisticInstance(id, "q0", id + "#0", 1L, probability, new double[] {x, y});
  }
}
