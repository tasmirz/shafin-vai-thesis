package com.thesis.topk.flink;

import com.thesis.topk.algorithm.DdImputationSynopsis;
import com.thesis.topk.algorithm.ImputationEngine;
import com.thesis.topk.model.OpType;
import com.thesis.topk.model.RawEvent;
import java.util.List;
import org.apache.flink.api.common.functions.RichMapFunction;

public final class FlinkImputationFunction extends RichMapFunction<RawEvent, ImputedRecord> {
  private final DdImputationSynopsis synopsis;

  public FlinkImputationFunction(DdImputationSynopsis synopsis) {
    this.synopsis = synopsis;
  }

  @Override
  public ImputedRecord map(RawEvent value) {
    return new ImputedRecord(
        value.objectId(),
        value.queryId(),
        value.eventTime(),
        value.opType(),
        value.opType() == OpType.DELETE ? List.of() : ImputationEngine.impute(value, synopsis));
  }
}
