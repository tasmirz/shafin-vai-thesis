package com.thesis.topk.algorithm;

import static org.assertj.core.api.Assertions.assertThat;

import com.thesis.topk.model.ImputationRule;
import com.thesis.topk.model.OpType;
import com.thesis.topk.model.ProbabilisticInstance;
import com.thesis.topk.model.RawEvent;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ImputationEngineTest {
  @Test
  void expandsMissingAttributeIntoNormalizedProbabilisticInstances() {
    RawEvent event = new RawEvent(
        "o1",
        "q0",
        1000L,
        new double[] {0.2, Double.NaN},
        new boolean[] {false, true},
        OpType.UPSERT);

    var instances = ImputationEngine.impute(
        event,
        Map.of(1, new ImputationRule(1, 0.1, 0.9, 0.7)));

    assertThat(instances).hasSize(2);
    assertThat(instances.get(0).probability())
        .isCloseTo(0.7, org.assertj.core.data.Offset.offset(1.0e-9));
    assertThat(instances.get(1).probability())
        .isCloseTo(0.3, org.assertj.core.data.Offset.offset(1.0e-9));
    assertThat(instances.stream().mapToDouble(ProbabilisticInstance::probability).sum())
        .isCloseTo(1.0, org.assertj.core.data.Offset.offset(1.0e-9));
    assertThat(instances.get(0).attributes()).containsExactly(0.2, 0.1);
    assertThat(instances.get(1).attributes()).containsExactly(0.2, 0.9);
  }

  @Test
  void completeRecordEmitsSingleCertainInstance() {
    RawEvent event = new RawEvent(
        "o1",
        "q0",
        1000L,
        new double[] {0.2, 0.4},
        new boolean[] {false, false},
        OpType.UPSERT);

    var instances = ImputationEngine.impute(event, Map.of());

    assertThat(instances).hasSize(1);
    assertThat(instances.get(0).probability()).isEqualTo(1.0);
    assertThat(instances.get(0).attributes()).containsExactly(0.2, 0.4);
  }
}
