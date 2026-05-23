package com.thesis.topk.algorithm;

import static org.assertj.core.api.Assertions.assertThat;

import com.thesis.topk.model.CandidateScore;
import com.thesis.topk.model.ProbabilisticInstance;
import com.thesis.topk.model.QueryPoint;
import com.thesis.topk.simulator.DummyDataSimulator;
import com.thesis.topk.simulator.SimulationConfig;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class ProbabilisticTopKTest {
  @Test
  void topKUsesScoreDescendingThenObjectIdTieBreak() {
    List<CandidateScore> top = ProbabilisticTopK.topK(List.of(
        new CandidateScore("b", "q0", 2.0, 2.0, 2.0, 1),
        new CandidateScore("a", "q0", 2.0, 2.0, 2.0, 1),
        new CandidateScore("c", "q0", 1.0, 1.0, 1.0, 1)), 2);

    assertThat(top).extracting(CandidateScore::objectId).containsExactly("a", "b");
  }

  @Test
  void prunedRankingAgreesWithExactRankingOnDeterministicSimulation() {
    var config = new SimulationConfig(40, 3, 1, 5, 0.2, 99L, 1000L, 10L);
    var data = DummyDataSimulator.generate(config);
    List<ProbabilisticInstance> instances = new ArrayList<>();
    data.events().forEach(event -> instances.addAll(ImputationEngine.impute(event, data.rules())));
    QueryPoint query = data.queryPoints().get("q0");

    List<CandidateScore> exact = ProbabilisticTopK.exactTopK(instances, query, config.k());
    ProbabilisticTopK.PrunedRanking pruned = ProbabilisticTopK.prunedTopK(instances, query, config.k());

    assertThat(pruned.topK()).extracting(CandidateScore::objectId)
        .containsExactlyElementsOf(exact.stream().map(CandidateScore::objectId).toList());
    assertThat(pruned.refinedCount()).isLessThanOrEqualTo(pruned.objectCount());
  }
}

