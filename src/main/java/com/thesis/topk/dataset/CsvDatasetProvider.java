package com.thesis.topk.dataset;

import com.thesis.topk.model.OpType;
import com.thesis.topk.model.QueryPoint;
import com.thesis.topk.model.RawEvent;
import com.thesis.topk.simulator.Args;
import com.thesis.topk.simulator.SimulationConfig;
import com.thesis.topk.simulator.SimulationData;
import java.io.BufferedReader;
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
    List<RawEvent> events = new ArrayList<>();
    Map<String, QueryPoint> queryPoints = new LinkedHashMap<>();
    int dimensions;
    try (BufferedReader reader = Files.newBufferedReader(path)) {
      String headerLine = reader.readLine();
      if (headerLine == null) {
        throw new IllegalArgumentException("csv dataset is empty: " + path);
      }
      String[] header = split(headerLine);
      int objectId = requiredColumn(header, "objectId");
      int instanceId = optionalColumn(header, "instanceId");
      int queryId = requiredColumn(header, "queryId");
      int eventTime = requiredColumn(header, "eventTime");
      int opType = optionalColumn(header, "opType");
      int probability = optionalColumn(header, "probability");
      int serverId = optionalColumn(header, "serverId");
      List<Integer> attributeColumns = attributeColumns(header);
      List<Integer> queryAttributeColumns = queryAttributeColumns(header, attributeColumns.size());
      List<Integer> mbrMinColumns = coordinateColumns(header, "mbrMin", attributeColumns.size());
      List<Integer> mbrMaxColumns = coordinateColumns(header, "mbrMax", attributeColumns.size());
      if (mbrMinColumns.isEmpty() != mbrMaxColumns.isEmpty()) {
        throw new IllegalArgumentException("csv dataset must provide both mbrMin and mbrMax columns");
      }
      if (attributeColumns.isEmpty()) {
        throw new IllegalArgumentException(
            "csv dataset must contain attribute columns after opType or named a0,a1,...");
      }
      dimensions = attributeColumns.size();

      String rawLine;
      while ((rawLine = reader.readLine()) != null) {
        String line = rawLine.trim();
        if (line.isEmpty() || line.startsWith("#")) {
          continue;
        }
        String[] cells = split(line);
        double[] attributes = new double[dimensions];
        boolean[] missing = new boolean[dimensions];
        for (int i = 0; i < dimensions; i++) {
          String value = cell(cells, attributeColumns.get(i));
          missing[i] = isMissing(value);
          attributes[i] = missing[i] ? Double.NaN : Double.parseDouble(value);
        }

        String qid = cell(cells, queryId);
        double[] mbrMin = coordinates(cells, mbrMinColumns);
        double[] mbrMax = coordinates(cells, mbrMaxColumns);
        events.add(new RawEvent(
            cell(cells, objectId),
            instanceId < 0 ? cell(cells, objectId) + "#raw" : cell(cells, instanceId),
            qid,
            Long.parseLong(cell(cells, eventTime)),
            probability < 0 ? 1.0 : Double.parseDouble(cell(cells, probability)),
            serverId < 0 ? -1 : Integer.parseInt(cell(cells, serverId)),
            attributes,
            missing,
            mbrMin,
            mbrMax,
            parseOpType(opType < 0 ? "" : cell(cells, opType))));
        queryPoints.computeIfAbsent(qid, id -> queryPoint(
            id, cells, queryAttributeColumns, dimensions));
      }
    }

    Map<String, QueryPoint> points = queryPoints.isEmpty()
        ? DatasetDefaults.queryPoints(config.queries(), dimensions)
        : queryPoints;
    validateAppearanceProbabilities(events);
    return new SimulationData(List.copyOf(events), DatasetDefaults.rules(dimensions), points);
  }

  private static void validateAppearanceProbabilities(List<RawEvent> events) {
    boolean paperStyle = events.stream().anyMatch(event -> event.appearanceProbability() != 1.0);
    if (!paperStyle) {
      return;
    }
    Map<String, Double> probabilityByObjectAndQuery = new LinkedHashMap<>();
    Map<String, Integer> serverByObject = new LinkedHashMap<>();
    for (RawEvent event : events) {
      String objectKey = event.queryId() + "|" + event.objectId();
      probabilityByObjectAndQuery.merge(objectKey, event.appearanceProbability(), Double::sum);
      if (event.serverPartition() >= 0) {
        Integer previous = serverByObject.putIfAbsent(event.objectId(), event.serverPartition());
        if (previous != null && previous != event.serverPartition()) {
          throw new IllegalArgumentException(
              "csv uncertain object spans server partitions: " + event.objectId());
        }
      }
    }
    for (Map.Entry<String, Double> entry : probabilityByObjectAndQuery.entrySet()) {
      if (Math.abs(entry.getValue() - 1.0) > 1.0e-6) {
        throw new IllegalArgumentException(
            "csv uncertain object probability does not sum to 1: " + entry.getKey());
      }
    }
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

  private static List<Integer> queryAttributeColumns(String[] header, int dimensions) {
    List<Integer> columns = new ArrayList<>();
    for (int d = 0; d < dimensions; d++) {
      int column = optionalColumn(header, "queryA" + d);
      if (column < 0) {
        return List.of();
      }
      columns.add(column);
    }
    return columns;
  }

  private static List<Integer> coordinateColumns(String[] header, String prefix, int dimensions) {
    List<Integer> columns = new ArrayList<>();
    for (int d = 0; d < dimensions; d++) {
      int column = optionalColumn(header, prefix + (d == 0 ? "X" : d == 1 ? "Y" : d));
      if (column < 0) {
        return List.of();
      }
      columns.add(column);
    }
    return columns;
  }

  private static double[] coordinates(String[] cells, List<Integer> columns) {
    double[] result = new double[columns.size()];
    for (int d = 0; d < columns.size(); d++) {
      result[d] = Double.parseDouble(cell(cells, columns.get(d)));
    }
    return result;
  }

  private static QueryPoint queryPoint(
      String queryId, String[] cells, List<Integer> queryColumns, int dimensions) {
    if (queryColumns.isEmpty()) {
      return DatasetDefaults.queryPoint(queryId, dimensions);
    }
    double[] coordinates = new double[dimensions];
    for (int d = 0; d < dimensions; d++) {
      coordinates[d] = Double.parseDouble(cell(cells, queryColumns.get(d)));
    }
    return new QueryPoint(queryId, coordinates);
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
