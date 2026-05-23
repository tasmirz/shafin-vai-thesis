package com.thesis.topk.flink;

import com.thesis.topk.algorithm.ImputationEngine;
import com.thesis.topk.model.ImputationRule;
import com.thesis.topk.model.RawEvent;
import java.util.Map;
import org.apache.flink.api.common.functions.RichMapFunction;

public final class FlinkImputationFunction extends RichMapFunction<RawEvent, ImputedRecord> {
  private final Map<Integer, ImputationRule> rules;

  public FlinkImputationFunction(Map<Integer, ImputationRule> rules) {
    this.rules = rules;
  }

  @Override
  public ImputedRecord map(RawEvent value) {
    return new ImputedRecord(value.queryId(), value.eventTime(), ImputationEngine.impute(value, rules));
  }
}

