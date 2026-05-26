package com.thesis.topk.algorithm.index;

import static org.assertj.core.api.Assertions.assertThat;

import com.thesis.topk.model.ProbabilisticInstance;
import com.thesis.topk.model.QueryPoint;
import java.util.List;
import org.junit.jupiter.api.Test;

class AggregateRTreeTest {
  private static final QueryPoint QUERY = new QueryPoint("q0", new double[] {0.0, 0.0});

  @Test
  void exposesFullyDominatedMassWithoutReducerWork() {
    AggregateRTree index = AggregateRTree.build(1, List.of(
        object("remote-a", 0.25, 0.25, 0.20, 0.30),
        object("remote-b", 0.45, 0.45, 0.40, 0.50)), 2);
    ProbabilisticInstance candidate = object("candidate", 0.10, 0.10, 0.09, 0.11);

    AggregateRTree.Inspection inspection = index.inspectAtLevel(candidate, QUERY, 0);

    assertThat(inspection.fullyDominatedMass()).isEqualTo(2.0);
    assertThat(inspection.partiallyDominatedNodes()).isEmpty();
  }

  @Test
  void reducerTraversalResolvesPartialMbrToExactDominatedProbability() {
    AggregateRTree index = AggregateRTree.build(1, List.of(
        object("remote-a", 0.25, 0.25, 0.05, 0.30),
        object("remote-b", 0.07, 0.07, 0.05, 0.30)), 2);
    ProbabilisticInstance candidate = object("candidate", 0.10, 0.10, 0.09, 0.11);

    AggregateRTree.Inspection inspection = index.inspectAtLevel(candidate, QUERY, 0);
    double exact = index.exactDominatedMass(
        candidate, inspection.partiallyDominatedNodes().get(0), QUERY);

    assertThat(inspection.fullyDominatedMass()).isZero();
    assertThat(inspection.partialUpperMass()).isEqualTo(2.0);
    assertThat(exact).isEqualTo(1.0);
  }

  @Test
  void localRootTraversalDoesNotCountTheCandidateObjectItself() {
    ProbabilisticInstance candidate = object("candidate", 0.10, 0.10, 0.09, 0.11);
    AggregateRTree index = AggregateRTree.build(1, List.of(
        candidate,
        object("dominated", 0.25, 0.25, 0.20, 0.30)), 2);

    double exact = index.exactDominatedMass(candidate, QUERY);

    assertThat(exact).isEqualTo(1.0);
  }

  @Test
  void selectsAnExportedLevelAndReportsEstimatedCommunicationCost() {
    List<ProbabilisticInstance> remote = java.util.stream.IntStream.range(0, 20)
        .mapToObj(i -> object("remote-" + i, 0.20 + i * 0.01, 0.20 + i * 0.01, 0.05, 0.50))
        .toList();
    AggregateRTree index = AggregateRTree.build(2, remote, 4);

    AggregateRTree.LevelSelection selection = index.selectExportLevel(
        List.of(object("candidate", 0.10, 0.10, 0.09, 0.11)), QUERY);

    assertThat(selection.partitionId()).isEqualTo(2);
    assertThat(selection.exportedNodes()).isPositive();
    assertThat(selection.estimatedCommunicationCost())
        .isEqualTo(selection.exportedNodes() + selection.estimatedPartialReferences());
  }

  private static ProbabilisticInstance object(
      String id, double x, double y, double minimum, double maximum) {
    return new ProbabilisticInstance(
        id, "q0", id + "#0", 1L, 1.0, 1, new double[] {x, y},
        new double[] {minimum, minimum}, new double[] {maximum, maximum});
  }
}
