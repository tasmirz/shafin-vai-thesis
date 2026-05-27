package com.thesis.topk.algorithm.index;

import static org.assertj.core.api.Assertions.assertThat;

import com.thesis.topk.model.ProbabilisticInstance;
import com.thesis.topk.model.QueryPoint;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
    assertThat(selection.estimatedTraversalObjects()).isNotNegative();
    assertThat(selection.estimatedCommunicationCost())
        .isEqualTo(selection.exportedNodes()
            + selection.estimatedPartialReferences()
            + selection.estimatedTraversalObjects());
  }

  @Test
  void spatialPackingKeepsSeparatedRowsOutOfPartialLeafMbrs() {
    List<ProbabilisticInstance> remote = new ArrayList<>();
    for (int index = 0; index < 16; index++) {
      double x = 0.60 + (index / 2) * 0.01;
      double y = index % 2 == 0 ? 0.20 : 0.80;
      remote.add(pointObject("remote-" + index, x, y));
    }
    AggregateRTree tree = AggregateRTree.build(1, remote, 4);
    ProbabilisticInstance candidate = pointObject("candidate", 0.50, 0.50);

    AggregateRTree.Inspection inspection = tree.inspectAtLevel(candidate, QUERY, 0);

    assertThat(inspection.partiallyDominatedNodes()).isEmpty();
    assertThat(inspection.fullyDominatedMass()).isEqualTo(8.0);
  }

  @Test
  void visitsPromisingCandidateLeavesFirstForMapperThresholdPruning() {
    AggregateRTree local = AggregateRTree.build(0, List.of(
        object("near", 0.10, 0.10, 0.08, 0.12),
        object("near-2", 0.12, 0.12, 0.10, 0.14),
        object("far", 0.90, 0.90, 0.88, 0.92),
        object("far-2", 0.92, 0.92, 0.90, 0.94)), 2);
    AggregateRTree remote = AggregateRTree.build(1, List.of(
        object("remote", 0.40, 0.40, 0.38, 0.42)), 2);

    List<AggregateRTree.ObjectCandidate> ordered = local.bestFirstObjectCandidates(
        Map.of(0, local, 1, remote), QUERY);

    assertThat(ordered).extracting(AggregateRTree.ObjectCandidate::objectId)
        .containsExactly("near", "near-2", "far", "far-2");
    assertThat(ordered.get(0).traversalUpperBound())
        .isGreaterThan(ordered.get(2).traversalUpperBound());
  }

  @Test
  void stopsHeapTraversalAndReportsSkippedSubtreeObjects() {
    AggregateRTree local = AggregateRTree.build(0, List.of(
        object("near", .10, .10, .08, .12),
        object("near-2", .12, .12, .10, .14),
        object("far", .90, .90, .88, .92),
        object("far-2", .92, .92, .90, .94)), 2);
    AggregateRTree remote = AggregateRTree.build(1, List.of(
        object("remote", .40, .40, .38, .42)), 2);
    List<String> visited = new ArrayList<>();

    List<String> skipped = local.visitBestFirstObjectCandidates(
        Map.of(0, local, 1, remote), QUERY, candidate -> {
          visited.add(candidate.objectId());
          return visited.size() < 3;
        });

    assertThat(visited).containsExactly("near", "near-2", "far");
    assertThat(skipped).containsExactlyInAnyOrder("far", "far-2");
  }

  private static ProbabilisticInstance object(
      String id, double x, double y, double minimum, double maximum) {
    return new ProbabilisticInstance(
        id, "q0", id + "#0", 1L, 1.0, 1, new double[] {x, y},
        new double[] {minimum, minimum}, new double[] {maximum, maximum});
  }

  private static ProbabilisticInstance pointObject(String id, double x, double y) {
    return new ProbabilisticInstance(
        id, "q0", id + "#0", 1L, 1.0, 1, new double[] {x, y},
        new double[] {x, y}, new double[] {x, y});
  }
}
