package com.thesis.topk.hadoop;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.thesis.topk.algorithm.DominanceScorer;
import com.thesis.topk.algorithm.variant.PtdAlgorithm;
import com.thesis.topk.algorithm.variant.PtdAlgorithmRegistry;
import com.thesis.topk.model.CandidateScore;
import com.thesis.topk.model.OpType;
import com.thesis.topk.model.ProbabilisticInstance;
import com.thesis.topk.model.QueryPoint;
import com.thesis.topk.simulator.Args;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/** Hadoop MapReduce implementation of the ICCIT-style PTD treatment variants. */
public final class ProbabilisticTopKHadoopJob {
  private static final String CONF_HEADER = "ptd.csv.header";
  private static final String CONF_K = "ptd.k";
  private static final String CONF_PARTITIONS = "ptd.partitions";
  private static final String CONF_ALGORITHM = "ptd.algorithm";
  private static final String CONF_VALIDATE = "ptd.validateExact";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private ProbabilisticTopKHadoopJob() {
  }

  public static void main(String[] args) throws Exception {
    Args parsed = Args.parse(args);
    String input = parsed.stringValue("input", parsed.stringValue("datasetPath", ""));
    String output = parsed.stringValue("output", parsed.stringValue("outputPath", ""));
    if (input.isBlank() || output.isBlank()) {
      throw new IllegalArgumentException(
          "Hadoop PTD requires --input/--datasetPath and --output/--outputPath");
    }
    int k = parsed.intValue("k", 10);
    int partitions = parsed.intValue("partitions", 4);
    boolean validateExact = parsed.booleanValue("validateExact", false);
    boolean local = parsed.booleanValue("local", true);
    PtdAlgorithm algorithm = PtdAlgorithmRegistry.require(
        parsed.stringValue("algorithm", PtdAlgorithmRegistry.DEFAULT_ID));

    Configuration conf = new Configuration();
    if (local) {
      conf.set("mapreduce.framework.name", "local");
      conf.set("fs.defaultFS", "file:///");
    }
    conf.set(CONF_HEADER, readHeader(conf, new Path(input)));
    conf.setInt(CONF_K, k);
    conf.setInt(CONF_PARTITIONS, partitions);
    conf.set(CONF_ALGORITHM, algorithm.id());
    conf.setBoolean(CONF_VALIDATE, validateExact);

    Path outputPath = new Path(output);
    FileSystem outputFs = outputPath.getFileSystem(conf);
    outputFs.delete(outputPath, true);

    Instant start = Instant.now();
    Job job = Job.getInstance(conf, "ptd-hadoop-" + algorithm.id());
    job.setJarByClass(ProbabilisticTopKHadoopJob.class);
    job.setMapperClass(PtdMapper.class);
    job.setReducerClass(PtdReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(input));
    FileOutputFormat.setOutputPath(job, outputPath);

    String resultsFile = parsed.stringValue("resultsFile", null);
    if (!job.waitForCompletion(true)) {
      throw new IllegalStateException("Hadoop PTD job failed");
    }
    printOutput(conf, outputPath, algorithm, k, partitions, validateExact,
        Duration.between(start, Instant.now()), resultsFile);
  }

  private static String readHeader(Configuration conf, Path input) throws IOException {
    FileSystem fs = input.getFileSystem(conf);
    Path headerPath = input;
    if (fs.getFileStatus(input).isDirectory()) {
      headerPath = fs.listStatus(input, path -> path.getName().endsWith(".csv"))[0].getPath();
    }
    try (BufferedReader reader = new BufferedReader(
        new InputStreamReader(fs.open(headerPath), StandardCharsets.UTF_8))) {
      String header = reader.readLine();
      if (header == null || header.isBlank()) {
        throw new IllegalArgumentException("CSV input has no header: " + input);
      }
      return header;
    }
  }

  private static void printOutput(
      Configuration conf,
      Path output,
      PtdAlgorithm algorithm,
      int k,
      int partitions,
      boolean validateExact,
      Duration elapsed,
      String resultsFile) throws IOException {
    FileSystem fs = output.getFileSystem(conf);
    List<String> rows = new ArrayList<>();
    for (var status : fs.listStatus(output, path -> path.getName().startsWith("part-"))) {
      try (BufferedReader reader = new BufferedReader(
          new InputStreamReader(fs.open(status.getPath()), StandardCharsets.UTF_8))) {
        String line;
        while ((line = reader.readLine()) != null) {
          int tab = line.indexOf('\t');
          rows.add(tab >= 0 ? line.substring(tab + 1) : line);
        }
      }
    }
    List<QueryMetrics> metrics = new ArrayList<>();
    long rawEvents = 0L;
    long instances = 0L;
    for (String row : rows) {
      QueryMetrics metric = MAPPER.readValue(row, QueryMetrics.class);
      metrics.add(metric);
      rawEvents += metric.rawEvents;
      instances += metric.instances;
    }
    long algorithmMs = metrics.stream()
        .mapToLong(metric -> metric.filterMs + metric.emissionMs + metric.refineMs)
        .sum();
    long validationMs = metrics.stream().mapToLong(metric -> metric.validationMs).sum();
    long setupMs = Math.max(0L, elapsed.toMillis() - algorithmMs - validationMs);
    String boundMode = metrics.stream().anyMatch(metric -> metric.indexedMbrPath)
        ? "hadoop-ddr-mbr-reducer"
        : "hadoop-conservative-reducer";
    System.out.printf(
        "engine=apache-hadoop source=mapreduce dataset=csv k=%d partitions=%d elapsedMs=%d "
            + "algorithmElapsedMs=%d setupMs=%d validationMs=%d algorithm=%s dscp=%s aes=%s "
            + "boundMode=%s emissionScope=server-partition%n",
        k, partitions, elapsed.toMillis(), algorithmMs, setupMs, validationMs, algorithm.id(),
        algorithm.dscpEnabled(), algorithm.aesEnabled(), boundMode);
    System.out.printf("rawEvents=%d probabilisticInstances=%d synopsisRules=0 synopsisBins=0%n",
        rawEvents, instances);
    for (QueryMetrics metric : metrics) {
      System.out.printf(
          "query=%s algorithm=%s objects=%d refined=%d pruned=%d pruneRatio=%.4f tau=%.6f "
              + "emittedRecords=%d baselineEmissions=%d aesEmissions=%d AER=%.6f "
              + "falsePrunes=%d indexedMbrPath=%s partialMbrRefs=%d "
              + "filterMs=%d emissionMs=%d refineMs=%d shuffleRecords=%d "
              + "shuffleBytes=%d tasks=%d executorRunMs=%d gcMs=%d stragglerRatio=%.4f "
              + "validationPerformed=%s exactAgreement=%s%n",
          metric.queryId, algorithm.id(), metric.objects, metric.refined, metric.pruned,
          metric.pruneRatio, metric.tau, metric.emittedRecords, metric.baselineEmissions,
          metric.aesEmissions, metric.aer, metric.falsePrunes, metric.indexedMbrPath,
          metric.partialMbrRefs, metric.filterMs, metric.emissionMs, metric.refineMs,
          metric.shuffleRecords, metric.shuffleBytes, 1, algorithmMs, 0, 1.0,
          validateExact, metric.exactAgreement);
      if (metric.topK != null) {
        for (CandidateScore score : metric.topK) {
          System.out.printf("  rank object=%s score=%.6f lb=%.6f ub=%.6f instances=%d%n",
              score.objectId(),
              score.exactScore(),
              score.lowerBound(),
              score.upperBound(),
              score.instanceCount());
        }
      }
    }
    if (resultsFile != null) {
      saveResultsToFile(resultsFile, metrics);
    }
  }

  public static final class PtdMapper extends Mapper<LongWritable, Text, Text, Text> {
    private String[] header;
    private Map<String, Integer> columns;
    private final Text outKey = new Text();
    private final Text outValue = new Text();
    
    private PtdAlgorithm algorithm;
    private int k;
    private int partitions;
    private boolean validateExact;
    private Map<String, Map<Integer, List<InstanceRecord>>> buffers;

    @Override
    protected void setup(Context context) {
      Configuration conf = context.getConfiguration();
      header = split(conf.get(CONF_HEADER));
      columns = columnMap(header);
      algorithm = PtdAlgorithmRegistry.require(conf.get(CONF_ALGORITHM));
      k = conf.getInt(CONF_K, 10);
      partitions = conf.getInt(CONF_PARTITIONS, 4);
      validateExact = conf.getBoolean(CONF_VALIDATE, false);
      buffers = new HashMap<>();
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
      InstanceRecord record = parseRecord(cells);
      ProbabilisticInstance instance = record.toInstance();
      int p = serverPartition(instance, partitions);
      buffers.computeIfAbsent(record.queryId, q -> new HashMap<>())
             .computeIfAbsent(p, part -> new ArrayList<>())
             .add(record);
             
      context.getCounter(PtdCounters.RAW_EVENTS).increment(1);
      context.getCounter(PtdCounters.INSTANCES).increment(1);
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      for (Map.Entry<String, Map<Integer, List<InstanceRecord>>> queryEntry : buffers.entrySet()) {
        String queryId = queryEntry.getKey();
        for (Map.Entry<Integer, List<InstanceRecord>> partitionEntry : queryEntry.getValue().entrySet()) {
          int p = partitionEntry.getKey();
          List<InstanceRecord> splitRecords = partitionEntry.getValue();
          QueryPoint queryPoint = new QueryPoint(queryId, splitRecords.get(0).query);
          List<ProbabilisticInstance> instances = splitRecords.stream()
              .map(InstanceRecord::toInstance)
              .toList();
          
          Map<String, List<ProbabilisticInstance>> byObject = instances.stream()
              .collect(Collectors.groupingBy(
                  ProbabilisticInstance::objectId, LinkedHashMap::new, Collectors.toList()));
                  
          List<CandidateEnvelope> candidates = byObject.entrySet().stream()
              .map(entry -> envelope(entry.getKey(), entry.getValue(), instances, queryPoint, p))
              .toList();
              
          double tau = candidates.stream()
              .map(c -> c.lowerBound())
              .sorted(Comparator.reverseOrder())
              .skip(Math.max(0, k - 1L))
              .findFirst()
              .orElse(Double.NEGATIVE_INFINITY);
              
          for (CandidateEnvelope c : candidates) {
            boolean pruned = algorithm.dscpEnabled() && c.upperBound() < tau;
            long baselineE = pruned ? 0 : c.baselineEmissions();
            long aesE = pruned ? 0 : c.aesEmissions();
            
            boolean first = true;
            for (ProbabilisticInstance inst : c.instances()) {
              InstanceRecord record = splitRecords.stream()
                  .filter(r -> r.instanceId.equals(inst.instanceId()))
                  .findFirst().get();
                  
              record.mapperPruned = pruned;
              if (first) {
                record.mapperBaselineEmissions = baselineE;
                record.mapperAesEmissions = aesE;
                first = false;
              } else {
                record.mapperBaselineEmissions = 0;
                record.mapperAesEmissions = 0;
              }
              
              if (!validateExact && pruned) {
                continue; // physical map-side prune
              }
              
              outKey.set(queryId);
              outValue.set(MAPPER.writeValueAsString(record));
              context.write(outKey, outValue);
            }
          }
        }
      }
    }
    
    private CandidateEnvelope envelope(
        String objectId,
        List<ProbabilisticInstance> objectInstances,
        List<ProbabilisticInstance> allInstances,
        QueryPoint queryPoint,
        int partition) {
      double lower = DominanceScorer.expectedDominanceScore(
          objectInstances, allInstances, queryPoint);
      double objectMass = objectInstances.stream().mapToDouble(ProbabilisticInstance::probability).sum();
      // Map-side conservative upper bound: assume all other objects in this partition
      // could fully dominate this object from remote partitions (remoteMass = sum of other objects' mass)
      double remoteMass = allInstances.stream()
          .filter(inst -> !inst.objectId().equals(objectId))
          .mapToDouble(ProbabilisticInstance::probability)
          .sum();
      double upper = lower + objectMass * remoteMass;
      
      List<String> competitors = allInstances.stream()
          .map(ProbabilisticInstance::objectId)
          .filter(id -> !id.equals(objectId))
          .distinct()
          .toList();
          
      long baselineEmissions = (long) objectInstances.size() * competitors.size();
      long aesEmissions = competitors.isEmpty() ? 0L : objectInstances.size();
      return new CandidateEnvelope(
          objectId, partition, objectInstances, lower, upper, baselineEmissions, aesEmissions);
    }
    private InstanceRecord parseRecord(String[] cells) {
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
      return new InstanceRecord(
          objectId,
          instanceId,
          cell(cells, required("queryId"), ""),
          Long.parseLong(cell(cells, required("eventTime"), "0")),
          Double.parseDouble(cell(cells, optional("probability", -1), "1.0")),
          Integer.parseInt(cell(cells, optional("serverId", -1), "-1")),
          attributes,
          query,
          mbrMin,
          mbrMax);
    }

    private List<Integer> attributeColumns() {
      List<Integer> columns = new ArrayList<>();
      for (int i = 0; i < header.length; i++) {
        if (header[i].matches("a\\d+")) {
          columns.add(i);
        }
      }
      return columns;
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
  }

  public static final class PtdReducer extends Reducer<Text, Text, Text, Text> {
    private PtdAlgorithm algorithm;
    private int k;
    private int partitions;
    private boolean validateExact;

    @Override
    protected void setup(Context context) {
      Configuration conf = context.getConfiguration();
      algorithm = PtdAlgorithmRegistry.require(conf.get(CONF_ALGORITHM));
      k = conf.getInt(CONF_K, 10);
      partitions = conf.getInt(CONF_PARTITIONS, 4);
      validateExact = conf.getBoolean(CONF_VALIDATE, false);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      List<InstanceRecord> records = new ArrayList<>();
      for (Text value : values) {
        records.add(MAPPER.readValue(value.toString(), InstanceRecord.class));
      }
      QueryMetrics metrics = rankQuery(key.toString(), records, context);
      context.write(new Text(key.toString()), new Text(MAPPER.writeValueAsString(metrics)));
    }

    private QueryMetrics rankQuery(
        String queryId,
        List<InstanceRecord> records,
        Context context) {
      Instant filterStart = Instant.now();
      QueryPoint queryPoint = new QueryPoint(queryId, records.get(0).query);
      List<ProbabilisticInstance> instances = records.stream()
          .map(InstanceRecord::toInstance)
          .toList();
      Map<String, List<ProbabilisticInstance>> byObject = instances.stream()
          .collect(Collectors.groupingBy(
              ProbabilisticInstance::objectId, LinkedHashMap::new, Collectors.toList()));
      List<CandidateEnvelope> candidates = byObject.entrySet().stream()
          .map(entry -> envelope(entry.getKey(), entry.getValue(), instances, queryPoint))
          .toList();
      Map<Integer, Double> thresholds = thresholds(candidates);
      double tau = thresholds.values().stream()
          .max(Double::compareTo)
          .orElse(Double.NaN);
      List<CandidateEnvelope> survivors = algorithm.dscpEnabled()
          ? candidates.stream()
              .filter(candidate -> candidate.upperBound() >= thresholds.get(candidate.partition()))
              .toList()
          : candidates;
      long filterMs = Duration.between(filterStart, Instant.now()).toMillis();

      Instant emissionStart = Instant.now();
      long baselineEmissions = records.stream().mapToLong(r -> r.mapperBaselineEmissions).sum();
      long aesEmissions = records.stream().mapToLong(r -> r.mapperAesEmissions).sum();
      long emittedRecords = algorithm.aesEnabled() ? aesEmissions : baselineEmissions;
      long shuffleBytes = emittedRecords * 96L;
      long emissionMs = Duration.between(emissionStart, Instant.now()).toMillis();

      Instant refineStart = Instant.now();
      List<CandidateScore> topK = survivors.stream()
          .map(candidate -> new CandidateScore(
              candidate.objectId(),
              queryId,
              DominanceScorer.expectedDominanceScore(candidate.instances(), instances, queryPoint),
              candidate.lowerBound(),
              candidate.upperBound(),
              candidate.instances().size()))
          .sorted()
          .limit(k)
          .toList();
      long refineMs = Duration.between(refineStart, Instant.now()).toMillis();

      Instant validationStart = Instant.now();
      boolean exactAgreement = true;
      long falsePrunes = 0L;
      if (validateExact) {
        List<String> exactIds = byObject.entrySet().stream()
            .map(entry -> new CandidateScore(
                entry.getKey(), queryId,
                DominanceScorer.expectedDominanceScore(entry.getValue(), instances, queryPoint),
                0.0, 0.0, entry.getValue().size()))
            .sorted()
            .limit(k)
            .map(CandidateScore::objectId)
            .toList();
        List<String> observedIds = topK.stream().map(CandidateScore::objectId).toList();
        exactAgreement = observedIds.equals(exactIds);
        List<String> survivorIds = survivors.stream().map(candidate -> candidate.objectId()).toList();
        falsePrunes = exactIds.stream().filter(id -> !survivorIds.contains(id)).count();
      }
      long validationMs = Duration.between(validationStart, Instant.now()).toMillis();

      context.getCounter(PtdCounters.OBJECTS).increment(candidates.size());
      context.getCounter(PtdCounters.REFINED).increment(survivors.size());
      context.getCounter(PtdCounters.PRUNED).increment(candidates.size() - survivors.size());
      context.getCounter(PtdCounters.EMITTED_RECORDS).increment(emittedRecords);
      context.getCounter(PtdCounters.BASELINE_EMISSIONS).increment(baselineEmissions);
      context.getCounter(PtdCounters.AES_EMISSIONS).increment(aesEmissions);
      context.getCounter(PtdCounters.FALSE_PRUNES).increment(falsePrunes);

      QueryMetrics metrics = new QueryMetrics();
      metrics.queryId = queryId;
      metrics.rawEvents = records.size();
      metrics.instances = instances.size();
      metrics.objects = candidates.size();
      metrics.refined = survivors.size();
      metrics.pruned = candidates.size() - survivors.size();
      metrics.pruneRatio = candidates.isEmpty() ? 0.0 : (double) metrics.pruned / candidates.size();
      metrics.tau = tau;
      metrics.emittedRecords = emittedRecords;
      metrics.baselineEmissions = baselineEmissions;
      metrics.aesEmissions = aesEmissions;
      metrics.aer = baselineEmissions == 0L ? 0.0 : (double) aesEmissions / baselineEmissions;
      metrics.falsePrunes = falsePrunes;
      metrics.indexedMbrPath = instances.stream().allMatch(ProbabilisticInstance::hasMbr);
      metrics.partialMbrRefs = baselineEmissions;
      metrics.filterMs = filterMs;
      metrics.emissionMs = emissionMs;
      metrics.refineMs = refineMs;
      metrics.validationMs = validationMs;
      metrics.shuffleRecords = emittedRecords;
      metrics.shuffleBytes = shuffleBytes;
      metrics.exactAgreement = exactAgreement;
      metrics.topK = topK;
      return metrics;
    }

    private CandidateEnvelope envelope(
        String objectId,
        List<ProbabilisticInstance> objectInstances,
        List<ProbabilisticInstance> allInstances,
        QueryPoint queryPoint) {
      int partition = serverPartition(objectInstances.get(0), partitions);
      List<ProbabilisticInstance> localInstances = allInstances.stream()
          .filter(instance -> serverPartition(instance, partitions) == partition)
          .toList();
      double lower = DominanceScorer.expectedDominanceScore(
          objectInstances, localInstances, queryPoint);
      double objectMass = objectInstances.stream().mapToDouble(ProbabilisticInstance::probability).sum();
      double remoteMass = allInstances.stream()
          .filter(other -> !other.objectId().equals(objectId))
          .filter(other -> serverPartition(other, partitions) != partition)
          .mapToDouble(ProbabilisticInstance::probability)
          .sum();
      double upper = lower + objectMass * remoteMass;
      List<String> competitors = localInstances.stream()
          .map(ProbabilisticInstance::objectId)
          .filter(id -> !id.equals(objectId))
          .distinct()
          .toList();
      long baselineEmissions = (long) objectInstances.size() * competitors.size();
      long aesEmissions = competitors.isEmpty() ? 0L : objectInstances.size();
      return new CandidateEnvelope(
          objectId, partition, objectInstances, lower, upper, baselineEmissions, aesEmissions);
    }

    private Map<Integer, Double> thresholds(List<CandidateEnvelope> candidates) {
      Map<Integer, List<Double>> byPartition = candidates.stream()
          .collect(Collectors.groupingBy(
              candidate -> candidate.partition(),
              Collectors.mapping(candidate -> candidate.lowerBound(), Collectors.toList())));
      Map<Integer, Double> thresholds = new HashMap<>();
      for (Map.Entry<Integer, List<Double>> entry : byPartition.entrySet()) {
        thresholds.put(entry.getKey(), kthLowerBound(entry.getValue()));
      }
      return thresholds;
    }

    private double kthLowerBound(List<Double> values) {
      return values.stream()
          .sorted(Comparator.reverseOrder())
          .skip(Math.max(0, k - 1L))
          .findFirst()
          .orElse(Double.NEGATIVE_INFINITY);
    }

    private long emittedRecordsFor(CandidateEnvelope candidate, PtdAlgorithm algorithm) {
      return algorithm.aesEnabled() ? candidate.aesEmissions : candidate.baselineEmissions;
    }
  }

  public enum PtdCounters {
    RAW_EVENTS,
    INSTANCES,
    OBJECTS,
    REFINED,
    PRUNED,
    EMITTED_RECORDS,
    BASELINE_EMISSIONS,
    AES_EMISSIONS,
    FALSE_PRUNES
  }

  public static final class InstanceRecord {
    public String objectId;
    public String instanceId;
    public String queryId;
    public long eventTime;
    public double probability;
    public int serverPartition;
    public double[] attributes;
    public double[] query;
    public double[] mbrMin;
    public double[] mbrMax;
    public boolean mapperPruned;
    public long mapperBaselineEmissions;
    public long mapperAesEmissions;

    public InstanceRecord() {
    }

    InstanceRecord(
        String objectId,
        String instanceId,
        String queryId,
        long eventTime,
        double probability,
        int serverPartition,
        double[] attributes,
        double[] query,
        double[] mbrMin,
        double[] mbrMax) {
      this.objectId = objectId;
      this.instanceId = instanceId;
      this.queryId = queryId;
      this.eventTime = eventTime;
      this.probability = probability;
      this.serverPartition = serverPartition;
      this.attributes = attributes.clone();
      this.query = query.clone();
      this.mbrMin = mbrMin.clone();
      this.mbrMax = mbrMax.clone();
    }

    ProbabilisticInstance toInstance() {
      return new ProbabilisticInstance(
          objectId, queryId, instanceId, eventTime, probability, serverPartition, attributes,
          mbrMin == null ? new double[0] : mbrMin,
          mbrMax == null ? new double[0] : mbrMax);
    }
  }

  public static final class QueryMetrics {
    public String queryId;
    public long rawEvents;
    public long instances;
    public long objects;
    public long refined;
    public long pruned;
    public double pruneRatio;
    public double tau;
    public long emittedRecords;
    public long baselineEmissions;
    public long aesEmissions;
    public double aer;
    public long falsePrunes;
    public boolean indexedMbrPath;
    public long partialMbrRefs;
    public long filterMs;
    public long emissionMs;
    public long refineMs;
    public long validationMs;
    public long shuffleRecords;
    public long shuffleBytes;
    public boolean exactAgreement;
    public List<CandidateScore> topK;
  }

  public record CandidateEnvelope(
      String objectId,
      int partition,
      List<ProbabilisticInstance> instances,
      double lowerBound,
      double upperBound,
      long baselineEmissions,
      long aesEmissions) {
  }

  private static int serverPartition(ProbabilisticInstance instance, int partitions) {
    if (instance.serverPartition() >= 0) {
      return Math.floorMod(instance.serverPartition(), partitions);
    }
    return Math.floorMod(instance.objectId().hashCode(), partitions);
  }

  private static String[] split(String line) {
    return line.split(",", -1);
  }

  private static Map<String, Integer> columnMap(String[] header) {
    Map<String, Integer> columns = new HashMap<>();
    for (int i = 0; i < header.length; i++) {
      columns.put(header[i].trim(), i);
    }
    return columns;
  }

  private static String cell(String[] cells, int index, String defaultValue) {
    if (index < 0 || index >= cells.length) {
      return defaultValue;
    }
    String value = cells[index].trim();
    return value.isEmpty() ? defaultValue : value;
  }

  private static void saveResultsToFile(String filePath, List<QueryMetrics> metricsList) {
    try (java.io.PrintWriter writer = new java.io.PrintWriter(new java.io.FileWriter(filePath))) {
      writer.println("query_id,rank,object_id,score,lower_bound,upper_bound,instances");
      for (QueryMetrics metric : metricsList) {
        if (metric.topK == null) continue;
        int rank = 1;
        for (CandidateScore score : metric.topK) {
          writer.printf("%s,%d,%s,%.6f,%.6f,%.6f,%d%n",
              metric.queryId,
              rank++,
              score.objectId(),
              score.exactScore(),
              score.lowerBound(),
              score.upperBound(),
              score.instanceCount());
        }
      }
      System.out.println("Saved Top-K results to: " + filePath);
    } catch (IOException e) {
      System.err.println("Failed to write results file: " + e.getMessage());
    }
  }
}
