package com.thesis.topk.simulator;

import com.thesis.topk.model.ImputationRule;
import com.thesis.topk.model.QueryPoint;
import com.thesis.topk.model.RawEvent;
import java.util.List;
import java.util.Map;

public record SimulationData(
    List<RawEvent> events,
    Map<Integer, ImputationRule> rules,
    Map<String, QueryPoint> queryPoints) {
}

