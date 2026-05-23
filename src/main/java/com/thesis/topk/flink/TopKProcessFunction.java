package com.thesis.topk.flink;

import com.thesis.topk.algorithm.ProbabilisticTopK;
import com.thesis.topk.model.OpType;
import com.thesis.topk.model.ProbabilisticInstance;
import com.thesis.topk.model.QueryPoint;
import com.thesis.topk.model.TopKResult;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public final class TopKProcessFunction extends KeyedProcessFunction<String, ImputedRecord, TopKResult> {
  private final Map<String, QueryPoint> queryPoints;
  private final int k;
  private final long windowMs;
  private transient ListState<ProbabilisticInstance> instances;
  private transient ValueState<Long> latestEventTime;

  public TopKProcessFunction(Map<String, QueryPoint> queryPoints, int k, long windowMs) {
    this.queryPoints = queryPoints;
    this.k = k;
    this.windowMs = windowMs;
  }

  @Override
  public void open(OpenContext openContext) {
    instances = getRuntimeContext().getListState(
        new ListStateDescriptor<>("instances", ProbabilisticInstance.class));
    latestEventTime = getRuntimeContext().getState(
        new ValueStateDescriptor<>("latest-event-time", Long.class));
  }

  @Override
  public void processElement(ImputedRecord value, Context ctx, Collector<TopKResult> out)
      throws Exception {
    long latest = latestEventTime.value() == null
        ? value.eventTime()
        : Math.max(latestEventTime.value(), value.eventTime());
    latestEventTime.update(latest);
    List<ProbabilisticInstance> live = liveSince(latest - windowMs);
    live.removeIf(instance -> instance.objectId().equals(value.objectId()));
    if (value.opType() != OpType.DELETE) {
      live.addAll(value.instances());
    }
    instances.update(live);
    ctx.timerService().registerEventTimeTimer(latest + windowMs);
    emitRanking(ctx.getCurrentKey(), latest, live, out);
  }

  @Override
  public void onTimer(long timestamp, OnTimerContext ctx, Collector<TopKResult> out)
      throws Exception {
    List<ProbabilisticInstance> live = liveSince(timestamp - windowMs);
    instances.update(live);
  }

  private List<ProbabilisticInstance> liveSince(long cutoff) throws Exception {
    List<ProbabilisticInstance> live = new ArrayList<>();
    for (ProbabilisticInstance instance : instances.get()) {
      if (instance.eventTime() >= cutoff) {
        live.add(instance);
      }
    }
    return live;
  }

  private void emitRanking(
      String queryId,
      long eventTime,
      List<ProbabilisticInstance> live,
      Collector<TopKResult> out) {
    QueryPoint queryPoint = queryPoints.get(queryId);
    if (queryPoint == null) {
      return;
    }
    out.collect(new TopKResult(
        queryId,
        eventTime,
        live.isEmpty() ? List.of() : ProbabilisticTopK.prunedTopK(live, queryPoint, k).topK()));
  }
}
