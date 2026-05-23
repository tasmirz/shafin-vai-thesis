package com.thesis.topk.algorithm;

import static org.assertj.core.api.Assertions.assertThat;

import com.thesis.topk.model.OpType;
import com.thesis.topk.model.RawEvent;
import java.util.List;
import org.junit.jupiter.api.Test;

class DdImputationSynopsisTest {
  @Test
  void selectsConditionalRepairsFromObservedDeterminantBuckets() {
    DdImputationSynopsis synopsis = DdImputationSynopsis.train(List.of(
        event("a", 0.10, 0.12),
        event("b", 0.15, 0.17),
        event("c", 0.80, 0.82),
        event("d", 0.85, 0.87)), 2);
    RawEvent incomplete = new RawEvent(
        "e", "q0", 5L, new double[] {0.12, Double.NaN},
        new boolean[] {false, true}, OpType.UPSERT);

    var candidates = synopsis.candidates(incomplete, 1);
    var instances = ImputationEngine.impute(incomplete, synopsis);

    assertThat(synopsis.ruleCount()).isEqualTo(2);
    assertThat(candidates).extracting(DdImputationSynopsis.ValueCandidate::value)
        .allMatch(value -> value < 0.5);
    assertThat(instances).hasSize(candidates.size());
    assertThat(instances.stream().mapToDouble(instance -> instance.probability()).sum())
        .isCloseTo(1.0, org.assertj.core.data.Offset.offset(1.0e-9));
  }

  @Test
  void evaluatesMaskedHoldoutValuesForImputationAccuracy() {
    DdImputationSynopsis synopsis = DdImputationSynopsis.train(List.of(
        event("a", 0.10, 0.12),
        event("b", 0.15, 0.17),
        event("c", 0.80, 0.82),
        event("d", 0.85, 0.87)), 2);

    DdImputationSynopsis.Evaluation evaluation =
        synopsis.evaluate(List.of(event("h", 0.13, 0.14)));

    assertThat(evaluation.evaluatedValues()).isEqualTo(2);
    assertThat(evaluation.meanAbsoluteError()).isLessThan(0.05);
  }

  private static RawEvent event(String id, double first, double second) {
    return new RawEvent(
        id, "q0", 1L, new double[] {first, second},
        new boolean[] {false, false}, OpType.UPSERT);
  }
}
