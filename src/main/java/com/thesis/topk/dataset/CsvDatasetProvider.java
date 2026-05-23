package com.thesis.topk.dataset;

import com.thesis.topk.model.OpType;
import com.thesis.topk.model.QueryPoint;
import com.thesis.topk.model.RawEvent;
import com.thesis.topk.simulator.Args;
import com.thesis.topk.simulator.SimulationConfig;
import com.thesis.topk.simulator.SimulationData;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public final class CsvDatasetProvider implements DatasetProvider {
  @Override
  public String name() {
    return "csv";
  }

  @Override
  public SimulationData generate(SimulationConfig config, Args args) {
    String datasetPath = args.stringValue("datasetPath", "").trim();
    if (datasetPath.isEmpty()) {
      throw new IllegalArgumentException("csv dataset requires --datasetPath=/path/to/events.csv");
    }

    try {
      return readEvents(Path.of(datasetPath), config);
    } catch (IOException e) {
      throw new IllegalArgumentException("failed to read csv dataset: " + datasetPath, e);
    }
  }

  private SimulationData readEvents(Path path, SimulationConfig config) throws IOException {
    List<String> lines = Files.readAllLines(path);
    if (lines.isEmpty()) {
      throw new IllegalArgumentException("csv dataset is empty: " + path);
    }
    String[] header = split(lines.get(0));
    int objectId = requiredColumn(header, "objectId");
    int queryId = requiredColumn(header, "queryId");
    int eventTime = requiredColumn(header, "eventTime");
    int opType = optionalColumn(header, "opType");
    List<Integer> attributeColumns = attributeColumns(header);
    if (attributeColumns.isEmpty()) {
      throw new IllegalArgumentException("csv dataset must contain attribute columns after opType or named a0,a1,...");
    }

    List<RawEvent> events = new ArrayList<>();
    Map<String, QueryPoint> queryPoints = new LinkedHashMap<>();
    for (int row = 1; row < lines.size(); row++) {
      String line = lines.get(row).trim();
      if (line.isEmpty() || line.startsWith("#")) {
        continue;
      }
      String[] cells = split(line);
      double[] attributes = new double[attributeColumns.size()];
      boolean[] missing = new boolean[attributeColumns.size()];
      for (int i = 0; i < attributeColumns.size(); i++) {
        String value = cell(cells, attributeColumns.get(i));
        missing[i] = isMissing(value);
        attributes[i] = missing[i] ? Double.NaN : Double.parseDouble(value);
      }

      String qid = cell(cells, queryId);
      events.add(new RawEvent(
          cell(cells, objectId),
          qid,
          Long.parseLong(cell(cells, eventTime)),
          attributes,
          missing,
          parseOpType(opType < 0 ? "" : cell(cells, opType))));
      queryPoints.computeIfAbsent(qid, id -> DatasetDefaults.queryPoint(id, attributeColumns.size()));
    }

    int dimensions = attributeColumns.size();
    Map<String, QueryPoint> points = queryPoints.isEmpty()
        ? DatasetDefaults.queryPoints(config.queries(), dimensions)
        : queryPoints;
    return new SimulationData(List.copyOf(events), DatasetDefaults.rules(dimensions), points);
  }

  private static List<Integer> attributeColumns(String[] header) {
    List<Integer> columns = new ArrayList<>();
    for (int i = 0; i < header.length; i++) {
      if (header[i].matches("a\\d+")) {
        columns.add(i);
      }
    }
    if (!columns.isEmpty()) {
      return columns;
    }
    int opType = optionalColumn(header, "opType");
    int start = opType >= 0 ? opType + 1 : requiredColumn(header, "eventTime") + 1;
    for (int i = start; i < header.length; i++) {
      columns.add(i);
    }
    return columns;
  }

  private static OpType parseOpType(String value) {
    if (value == null || value.isBlank()) {
      return OpType.UPSERT;
    }
    return OpType.valueOf(value.trim().toUpperCase(Locale.ROOT));
  }

  private static boolean isMissing(String value) {
    return value == null
        || value.isBlank()
        || "?".equals(value.trim())
        || "null".equalsIgnoreCase(value.trim())
        || "nan".equalsIgnoreCase(value.trim());
  }

  private static String cell(String[] cells, int index) {
    if (index < 0 || index >= cells.length) {
      return "";
    }
    return cells[index].trim();
  }

  private static int requiredColumn(String[] header, String name) {
    int index = optionalColumn(header, name);
    if (index < 0) {
      throw new IllegalArgumentException("csv dataset is missing required column: " + name);
    }
    return index;
  }

  private static int optionalColumn(String[] header, String name) {
    for (int i = 0; i < header.length; i++) {
      if (header[i].equalsIgnoreCase(name)) {
        return i;
      }
    }
    return -1;
  }

  private static String[] split(String line) {
    return line.split(",", -1);
  }
}
