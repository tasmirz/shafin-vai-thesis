package com.thesis.topk.flink;

import static org.assertj.core.api.Assertions.assertThat;

import com.thesis.topk.algorithm.DdImputationSynopsis;
import com.thesis.topk.model.OpType;
import com.thesis.topk.model.RawEvent;
import java.util.List;
import org.junit.jupiter.api.Test;

class FlinkImputationFunctionTest {
  @Test
  void deleteRecordsCarryIdentityWithoutCreatingCandidateInstances() throws Exception {
    RawEvent training = event("training", OpType.UPSERT, 0.2, 0.4);
    FlinkImputationFunction function =
        new FlinkImputationFunction(DdImputationSynopsis.train(List.of(training), 2));

    ImputedRecord deleted = function.map(event("object-1", OpType.DELETE, 0.2, 0.4));

    assertThat(deleted.objectId()).isEqualTo("object-1");
    assertThat(deleted.opType()).isEqualTo(OpType.DELETE);
    assertThat(deleted.instances()).isEmpty();
  }

  private static RawEvent event(String objectId, OpType opType, double first, double second) {
    return new RawEvent(
        objectId,
        "q0",
        1000L,
        new double[] {first, second},
        new boolean[] {false, false},
        opType);
  }
}
