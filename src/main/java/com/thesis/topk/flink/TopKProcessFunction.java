package com.thesis.topk.flink;

import com.thesis.topk.algorithm.ProbabilisticTopK;
import com.thesis.topk.model.ProbabilisticInstance;
import com.thesis.topk.model.QueryPoint;
import com.thesis.topk.model.TopKResult;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public final class TopKProcessFunction extends KeyedProcessFunction<String, ImputedRecord, TopKResult> {
  private final Map<String, QueryPoint> queryPoints;
  private final int k;
  private final long windowMs;
  private transient ListState<ProbabilisticInstance> instances;

  public TopKProcessFunction(Map<String, QueryPoint> queryPoints, int k, long windowMs) {
    this.queryPoints = queryPoints;
    this.k = k;
    this.windowMs = windowMs;
  }

  @Override
  public void open(OpenContext openContext) {
    instances = getRuntimeContext().getListState(
        new ListStateDescriptor<>("instances", ProbabilisticInstance.class));
  }

  @Override
  public void processElement(ImputedRecord value, Context ctx, Collector<TopKResult> out)
      throws Exception {
    for (ProbabilisticInstance instance : value.instances()) {
      instances.add(instance);
    }
    ctx.timerService().registerEventTimeTimer(value.eventTime() + windowMs);
  }

  @Override
  public void onTimer(long timestamp, OnTimerContext ctx, Collector<TopKResult> out)
      throws Exception {
    QueryPoint queryPoint = queryPoints.get(ctx.getCurrentKey());
    if (queryPoint == null) {
      return;
    }
    List<ProbabilisticInstance> live = new ArrayList<>();
    long cutoff = timestamp - windowMs;
    for (ProbabilisticInstance instance : instances.get()) {
      if (instance.eventTime() >= cutoff) {
        live.add(instance);
      }
    }
    instances.update(live);
    if (!live.isEmpty()) {
      out.collect(new TopKResult(
          ctx.getCurrentKey(),
          timestamp,
          ProbabilisticTopK.prunedTopK(live, queryPoint, k).topK()));
    }
  }
}

