package com.thesis.topk.hadoop;

import com.thesis.topk.algorithm.index.AggregateRTree;
import com.thesis.topk.model.ProbabilisticInstance;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Phase-1 mapper for a two-phase Rai-Lian aR-tree pipeline.
 *
 * <p>This mapper reads CSV probabilistic events, assigns each to a server partition, and buffers
 * instances in memory. In {@link #cleanup(Context)} it builds one {@link AggregateRTree} per
 * partition and emits a serialized summary index for each partition.
 *
 * <p>A downstream {@code RaiLianIndexRankingReducer} (or equivalent Phase-2 job) consumes these
 * per-partition serialized indices to perform partition-aware score-bound computation.
 *
 * <p><b>Note:</b> This mapper requires all instances to carry MBR bounds via the CSV columns
 * {@code mbrMinX, mbrMinY, mbrMaxX, mbrMaxY}. Inputs without MBR metadata will trigger an
 * {@link IllegalArgumentException} from {@link AggregateRTree#build}.
 */
public final class RaiLianIndexBuilderMapper extends Mapper<LongWritable, Text, Text, Text> {

  static final String CONF_HEADER = "ptd.csv.header";
  static final String CONF_PARTITIONS = "ptd.partitions";

  private String[] header;
  private Map<String, Integer> columns;
  private int partitions;
  private final Text outKey = new Text();
  private final Text outValue = new Text();
  private Map<Integer, List<ProbabilisticInstance>> partitionBuffers;

  @Override
  protected void setup(Context context) {
    Configuration conf = context.getConfiguration();
    header = split(conf.get(CONF_HEADER));
    columns = columnMap(header);
    partitions = conf.getInt(CONF_PARTITIONS, 4);
    partitionBuffers = new HashMap<>();
  }

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String line = value.toString();
    if (line.isBlank() || line.startsWith("#") || line.equals(String.join(",", header))) {
      return;
    }
    String[] cells = split(line);
    if (!"UPSERT".equalsIgnoreCase(cell(cells, optional("opType", -1), "UPSERT"))) {
      return;
    }
    ProbabilisticInstance instance = parseInstance(cells);
    int p = serverPartition(instance, partitions);
    partitionBuffers.computeIfAbsent(p, k -> new ArrayList<>()).add(instance);

    context.getCounter("RaiLian", "RAW_EVENTS").increment(1);
    context.getCounter("RaiLian", "INSTANCES").increment(1);
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    for (Map.Entry<Integer, List<ProbabilisticInstance>> entry : partitionBuffers.entrySet()) {
      int partitionId = entry.getKey();
      List<ProbabilisticInstance> instances = entry.getValue();
      if (instances.isEmpty()) {
        continue;
      }
      AggregateRTree index = AggregateRTree.build(partitionId, instances);
      AggregateRTree summary = index.summaryOnly();
      outKey.set(String.valueOf(partitionId));
      outValue.set(RaiLianSerde.serializeIndex(summary));
      context.write(outKey, outValue);
    }
  }

  private ProbabilisticInstance parseInstance(String[] cells) {
    List<Integer> attributeColumns = attributeColumns();
    double[] attributes = new double[attributeColumns.size()];
    for (int i = 0; i < attributeColumns.size(); i++) {
      attributes[i] = Double.parseDouble(cell(cells, attributeColumns.get(i), "NaN"));
    }
    double[] query = new double[attributeColumns.size()];
    for (int i = 0; i < query.length; i++) {
      query[i] = Double.parseDouble(cell(cells, optional("queryA" + i, -1), "1.0"));
    }
    double[] mbrMin = coordinates(cells, "mbrMin", attributeColumns.size());
    double[] mbrMax = coordinates(cells, "mbrMax", attributeColumns.size());
    String objectId = cell(cells, required("objectId"), "");
    String instanceId = cell(cells, optional("instanceId", -1), objectId + "#raw");
    return new ProbabilisticInstance(
        objectId,
        cell(cells, required("queryId"), ""),
        instanceId,
        Long.parseLong(cell(cells, required("eventTime"), "0")),
        Double.parseDouble(cell(cells, optional("probability", -1), "1.0")),
        Integer.parseInt(cell(cells, optional("serverId", -1), "-1")),
        attributes,
        mbrMin.length == 0 ? new double[0] : mbrMin,
        mbrMax.length == 0 ? new double[0] : mbrMax);
  }

  private List<Integer> attributeColumns() {
    List<Integer> cols = new ArrayList<>();
    for (int i = 0; i < header.length; i++) {
      if (header[i].matches("a\\d+")) {
        cols.add(i);
      }
    }
    return cols;
  }

  private double[] coordinates(String[] cells, String prefix, int dimensions) {
    List<Integer> indexes = new ArrayList<>();
    for (int d = 0; d < dimensions; d++) {
      int column = optional(prefix + (d == 0 ? "X" : d == 1 ? "Y" : d), -1);
      if (column < 0) {
        return new double[0];
      }
      indexes.add(column);
    }
    double[] values = new double[dimensions];
    for (int d = 0; d < dimensions; d++) {
      values[d] = Double.parseDouble(cell(cells, indexes.get(d), "NaN"));
    }
    return values;
  }

  private int required(String name) {
    Integer index = columns.get(name);
    if (index == null) {
      throw new IllegalArgumentException("CSV header missing required column: " + name);
    }
    return index;
  }

  private int optional(String name, int defaultValue) {
    return columns.getOrDefault(name, defaultValue);
  }

  static int serverPartition(ProbabilisticInstance instance, int partitions) {
    if (instance.serverPartition() >= 0) {
      return Math.floorMod(instance.serverPartition(), partitions);
    }
    return Math.floorMod(instance.objectId().hashCode(), partitions);
  }

  static String[] split(String line) {
    return line.split(",", -1);
  }

  static Map<String, Integer> columnMap(String[] header) {
    Map<String, Integer> cols = new HashMap<>();
    for (int i = 0; i < header.length; i++) {
      cols.put(header[i].trim(), i);
    }
    return cols;
  }

  static String cell(String[] cells, int index, String defaultValue) {
    if (index < 0 || index >= cells.length) {
      return defaultValue;
    }
    String value = cells[index].trim();
    return value.isEmpty() ? defaultValue : value;
  }
}
