import re

with open("src/main/java/com/thesis/topk/hadoop/ProbabilisticTopKHadoopJob.java", "r") as f:
    text = f.read()

# Add fields to InstanceRecord
text = text.replace(
    "    public double[] mbrMax;\n\n    public InstanceRecord()",
    "    public double[] mbrMax;\n    public boolean mapperPruned;\n    public long mapperBaselineEmissions;\n    public long mapperAesEmissions;\n\n    public InstanceRecord()"
)

# Update Mapper
mapper_start = text.find("  public static final class PtdMapper extends Mapper<LongWritable, Text, Text, Text> {")
mapper_end = text.find("  public static final class PtdReducer extends Reducer<Text, Text, Text, Text> {")
mapper_old = text[mapper_start:mapper_end]

mapper_new = """  public static final class PtdMapper extends Mapper<LongWritable, Text, Text, Text> {
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
      double remoteMass = 0; // map-side partial bounds only see local mass, so remoteMass is 0
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
"""

# Keep parseRecord and others in Mapper
parse_methods = mapper_old[mapper_old.find("    private InstanceRecord parseRecord"):]
mapper_new += parse_methods

text = text[:mapper_start] + mapper_new + text[mapper_end:]

# Change CandidateEnvelope to public record
text = text.replace("  private record CandidateEnvelope(", "  public record CandidateEnvelope(")

# Update Reducer emissions
reducer_old = """      Instant emissionStart = Instant.now();
      long baselineEmissions = survivors.stream().mapToLong(c -> c.baselineEmissions).sum();
      long aesEmissions = survivors.stream().mapToLong(c -> c.aesEmissions).sum();
      long emittedRecords = algorithm.aesEnabled() ? aesEmissions : baselineEmissions;
      long shuffleBytes = survivors.stream()
          .mapToLong(candidate -> emittedRecordsFor(candidate, algorithm) * 96L)
          .sum();
      long emissionMs = Duration.between(emissionStart, Instant.now()).toMillis();"""

reducer_new = """      Instant emissionStart = Instant.now();
      long baselineEmissions = records.stream().mapToLong(r -> r.mapperBaselineEmissions).sum();
      long aesEmissions = records.stream().mapToLong(r -> r.mapperAesEmissions).sum();
      long emittedRecords = algorithm.aesEnabled() ? aesEmissions : baselineEmissions;
      long shuffleBytes = records.stream()
          .filter(r -> r.mapperBaselineEmissions > 0 || r.mapperAesEmissions > 0)
          .count() * 96L;
      long emissionMs = Duration.between(emissionStart, Instant.now()).toMillis();"""

text = text.replace(reducer_old, reducer_new)

# Update Reducer envelope calls (now uses accessors like baselineEmissions())
text = text.replace("c -> c.baselineEmissions", "c -> c.baselineEmissions()")
text = text.replace("c -> c.aesEmissions", "c -> c.aesEmissions()")
text = text.replace("candidate.upperBound >=", "candidate.upperBound() >=")
text = text.replace("candidate.objectId", "candidate.objectId()")
text = text.replace("candidate.lowerBound", "candidate.lowerBound()")
text = text.replace("candidate.upperBound,", "candidate.upperBound(),")
text = text.replace("candidate.instances.size()", "candidate.instances().size()")
text = text.replace("candidate.instances", "candidate.instances()")
text = text.replace("candidate.partition", "candidate.partition()")

with open("src/main/java/com/thesis/topk/hadoop/ProbabilisticTopKHadoopJob.java", "w") as f:
    f.write(text)
