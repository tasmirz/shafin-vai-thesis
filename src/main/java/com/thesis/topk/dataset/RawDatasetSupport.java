package com.thesis.topk.dataset;

import com.thesis.topk.model.OpType;
import com.thesis.topk.model.RawEvent;
import com.thesis.topk.simulator.Args;
import com.thesis.topk.simulator.SimulationConfig;
import com.thesis.topk.simulator.SimulationData;
import java.io.BufferedReader;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

final class RawDatasetSupport {
  private static final DateTimeFormatter PUMP_TS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

  private RawDatasetSupport() {
  }

  static int maxEvents(SimulationConfig config, Args args) {
    return args.intValue("maxEvents", Math.max(1, config.objects() * config.queries()));
  }

  static SimulationData data(List<RawEvent> events, int dimensions) {
    Set<String> queryIds = new LinkedHashSet<>();
    for (RawEvent event : events) {
      queryIds.add(event.queryId());
    }
    var queries = new java.util.LinkedHashMap<String, com.thesis.topk.model.QueryPoint>();
    for (String queryId : queryIds) {
      queries.put(queryId, DatasetDefaults.queryPoint(queryId, dimensions));
    }
    if (queries.isEmpty()) {
      queries.put("q0", DatasetDefaults.queryPoint("q0", dimensions));
    }
    return new SimulationData(List.copyOf(events), DatasetDefaults.rules(dimensions), queries);
  }

  static List<RawEvent> readWhitespace(
      BufferedReader reader,
      int maxEvents,
      RowMapper mapper) throws IOException {
    List<RawEvent> events = new ArrayList<>(maxEvents);
    String line;
    int row = 0;
    while ((line = reader.readLine()) != null && events.size() < maxEvents) {
      line = line.trim();
      if (line.isEmpty() || line.startsWith("#")) {
        continue;
      }
      if (row == 0 && !Character.isDigit(line.charAt(0)) && line.charAt(0) != '-') {
        row++;
        continue;
      }
      RawEvent event = mapper.map(line.split("\\s+"), events.size());
      if (event != null) {
        events.add(event);
      }
      row++;
    }
    return events;
  }

  static RawEvent event(String objectId, String queryId, long eventTime, double[] attributes) {
    boolean[] missing = new boolean[attributes.length];
    for (int i = 0; i < attributes.length; i++) {
      missing[i] = Double.isNaN(attributes[i]);
    }
    return new RawEvent(objectId, queryId, eventTime, attributes, missing, OpType.UPSERT);
  }

  static double value(String raw) {
    if (raw == null || raw.isBlank() || "?".equals(raw) || "null".equalsIgnoreCase(raw)
        || "nan".equalsIgnoreCase(raw)) {
      return Double.NaN;
    }
    return Double.parseDouble(raw);
  }

  static long intelTimestamp(String date, String time) {
    return Instant.parse(date + "T" + time + "Z").toEpochMilli();
  }

  static long pumpTimestamp(String timestamp) {
    return LocalDateTime.parse(timestamp, PUMP_TS).toInstant(ZoneOffset.UTC).toEpochMilli();
  }

  interface RowMapper {
    RawEvent map(String[] columns, int emittedIndex);
  }
}
