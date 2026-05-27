package com.thesis.topk.algorithm.index;

import com.thesis.topk.algorithm.DominanceScorer;
import com.thesis.topk.model.ProbabilisticInstance;
import com.thesis.topk.model.QueryPoint;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Packed aggregate R-tree over uncertain-object MBRs.
 *
 * <p>Each leaf entry is one uncertain object and stores its full instance list for exact
 * reducer traversal. Internal entries retain an aggregate probability mass, which is equal to
 * the summed object mass below that entry. For normalized uncertain objects, that aggregate is
 * also the object count used in the Rai-Lian score-bound equations.</p>
 */
public final class AggregateRTree implements Serializable {
  public static final int DEFAULT_FANOUT = 16;
  private static final int MAX_LEVEL_SELECTION_PROBES = 256;

  private final int partitionId;
  private final int fanout;
  private final Node root;
  private final Map<String, Node> nodesById;
  private final Map<String, Double> objectMassById;

  private AggregateRTree(
      int partitionId,
      int fanout,
      Node root,
      Map<String, Node> nodesById,
      Map<String, Double> objectMassById) {
    this.partitionId = partitionId;
    this.fanout = fanout;
    this.root = root;
    this.nodesById = nodesById;
    this.objectMassById = objectMassById;
  }

  public static AggregateRTree build(int partitionId, List<ProbabilisticInstance> instances) {
    return build(partitionId, instances, DEFAULT_FANOUT);
  }

  public static AggregateRTree build(
      int partitionId, List<ProbabilisticInstance> instances, int requestedFanout) {
    int fanout = Math.max(2, requestedFanout);
    Map<String, List<ProbabilisticInstance>> byObject = instances.stream()
        .collect(Collectors.groupingBy(
            ProbabilisticInstance::objectId, LinkedHashMap::new, Collectors.toList()));
    if (byObject.isEmpty()) {
      return new AggregateRTree(partitionId, fanout, null, new HashMap<>(), new HashMap<>());
    }
    List<ObjectEntry> objects = byObject.entrySet().stream()
        .map(entry -> ObjectEntry.from(entry.getKey(), entry.getValue()))
        .toList();
    Map<String, Double> objectMasses = objects.stream()
        .collect(Collectors.toMap(ObjectEntry::objectId, ObjectEntry::probabilityMass));
    Map<String, Node> nodeMap = new HashMap<>();
    List<Node> level = new ArrayList<>();
    int serial = 0;
    for (List<ObjectEntry> members : strGroups(
        objects, fanout, ObjectEntry::minimum, ObjectEntry::maximum)) {
      Node leaf = Node.leaf(nodeId(partitionId, 0, serial++), partitionId, members);
      level.add(leaf);
      nodeMap.put(leaf.id(), leaf);
    }
    int height = 0;
    while (level.size() > 1) {
      height++;
      List<Node> parents = new ArrayList<>();
      serial = 0;
      for (List<Node> children : strGroups(level, fanout, Node::minimum, Node::maximum)) {
        Node parent = Node.branch(nodeId(partitionId, height, serial++), partitionId, height, children);
        parents.add(parent);
        nodeMap.put(parent.id(), parent);
      }
      level = parents;
    }
    return new AggregateRTree(
        partitionId, fanout, level.get(0), new HashMap<>(nodeMap), new HashMap<>(objectMasses));
  }

  public int partitionId() {
    return partitionId;
  }

  public int fanout() {
    return fanout;
  }

  public int height() {
    return root == null ? 0 : root.level();
  }

  public int objectCount() {
    return root == null ? 0 : root.objectCount();
  }

  /**
   * Returns an index-distribution view carrying only aggregate MBR nodes and masses.
   *
   * <p>Remote filtering mappers do not need instance payloads or leaf object identifiers.
   * Full leaf state remains at the keyed reducer partition for partial-MBR traversal.</p>
   */
  public AggregateRTree summaryOnly() {
    if (root == null) {
      return this;
    }
    Map<String, Node> summarizedNodes = new HashMap<>();
    Node summarizedRoot = Node.summary(root, summarizedNodes);
    return new AggregateRTree(
        partitionId, fanout, summarizedRoot, summarizedNodes, new HashMap<>());
  }

  /**
   * Selects an exported summary level using the paper's communication-cost trade-off.
   *
   * <p>A finer level costs more index entries to distribute, but usually emits fewer partially
   * dominated entries. A deterministic bounded probe sample stands in for a query-log
   * distribution during finite benchmark execution; production deployments can pass
   * historically calibrated probes through the same API.</p>
   */
  public LevelSelection selectExportLevel(
      List<ProbabilisticInstance> probes, QueryPoint queryPoint) {
    if (root == null) {
      return new LevelSelection(partitionId, 0, 0, 0L, 0L, 0L);
    }
    LevelSelection selected = null;
    List<ProbabilisticInstance> sampledProbes = calibrationSample(probes);
    for (int level = 0; level <= height(); level++) {
      List<Node> exported = exportedNodes(level);
      long partialReferences = 0L;
      long estimatedTraversalObjects = 0L;
      for (ProbabilisticInstance probe : sampledProbes) {
        Inspection inspection = inspectAtLevel(probe, queryPoint, level);
        partialReferences += inspection.partiallyDominatedNodes().size();
        for (NodeReference reference : inspection.partiallyDominatedNodes()) {
          estimatedTraversalObjects += nodesById.get(reference.nodeId()).objectCount();
        }
      }
      long estimatedCost = partialReferences + exported.size() + estimatedTraversalObjects;
      LevelSelection candidate = new LevelSelection(
          partitionId, level, exported.size(), partialReferences, estimatedTraversalObjects,
          estimatedCost);
      if (selected == null
          || candidate.estimatedCommunicationCost() < selected.estimatedCommunicationCost()
          || (candidate.estimatedCommunicationCost() == selected.estimatedCommunicationCost()
              && candidate.level() < selected.level())) {
        selected = candidate;
      }
    }
    return selected;
  }

  private static List<ProbabilisticInstance> calibrationSample(List<ProbabilisticInstance> probes) {
    if (probes.size() <= MAX_LEVEL_SELECTION_PROBES) {
      return probes;
    }
    List<ProbabilisticInstance> sample = new ArrayList<>(MAX_LEVEL_SELECTION_PROBES);
    double stride = (double) probes.size() / MAX_LEVEL_SELECTION_PROBES;
    for (int i = 0; i < MAX_LEVEL_SELECTION_PROBES; i++) {
      sample.add(probes.get(Math.min(probes.size() - 1, (int) Math.floor(i * stride))));
    }
    return sample;
  }

  /** Calculates full and partial remote contributions from the selected exported index level. */
  public Inspection inspectAtLevel(
      ProbabilisticInstance candidate, QueryPoint queryPoint, int level) {
    if (root == null) {
      return new Inspection(0.0, new ArrayList<>());
    }
    InspectionAccumulator result = new InspectionAccumulator();
    inspectAtLevel(candidate, queryPoint, Math.max(0, Math.min(level, height())), root, result);
    return new Inspection(result.fullyDominatedMass, result.partiallyDominated);
  }

  private void inspectAtLevel(
      ProbabilisticInstance candidate,
      QueryPoint queryPoint,
      int level,
      Node node,
      InspectionAccumulator result) {
    if (DominanceScorer.dynamicallyDominatesMbrFully(
        candidate, node.minimum(), node.maximum(), queryPoint)) {
      double mass = node.probabilityMass();
      if (node.objectIds().contains(candidate.objectId())) {
        mass -= objectMassById.getOrDefault(candidate.objectId(), 0.0);
      }
      result.fullyDominatedMass += mass;
      return;
    }
    if (!DominanceScorer.dynamicallyDominatesMbrPossibly(
        candidate, node.minimum(), node.maximum(), queryPoint)) {
      return;
    }
    if (node.level() <= level || node.leaf()) {
      result.partiallyDominated.add(new NodeReference(
          partitionId, node.id(), node.level(), node.probabilityMass()));
      return;
    }
    for (Node child : node.children()) {
      inspectAtLevel(candidate, queryPoint, level, child, result);
    }
  }

  private static final class InspectionAccumulator {
    private double fullyDominatedMass;
    List<NodeReference> partiallyDominated = new ArrayList<>();
  }

  /**
   * Traverses one partially dominated node to calculate the exact mass dominated by an
   * instance. This is the filtering-reducer computation from Rai and Lian Algorithm 3.
   */
  public double exactDominatedMass(
      ProbabilisticInstance candidate, NodeReference reference, QueryPoint queryPoint) {
    Node node = nodesById.get(reference.nodeId());
    if (node == null) {
      throw new IllegalArgumentException("Unknown aR-tree node: " + reference.nodeId());
    }
    return exactDominatedMass(candidate, node, queryPoint);
  }

  /** Traverses the local index root without counting the candidate's own uncertain object. */
  public double exactDominatedMass(ProbabilisticInstance candidate, QueryPoint queryPoint) {
    return root == null ? 0.0 : exactDominatedMass(candidate, root, queryPoint);
  }

  /**
   * Visits local objects in descending conservative upper-bound order.
   *
   * <p>The filtering mapper can stop evaluating leaf objects once its current k-th lower-bound
   * threshold exceeds the upper bound inherited from the next heap entry. This mirrors the
   * heap-ordered local traversal of Rai and Lian Algorithm 2.</p>
   */
  public List<ObjectCandidate> bestFirstObjectCandidates(
      Map<Integer, AggregateRTree> indexes, QueryPoint queryPoint) {
    List<ObjectCandidate> candidates = new ArrayList<>(objectCount());
    visitBestFirstObjectCandidates(indexes, queryPoint, candidate -> {
      candidates.add(candidate);
      return true;
    });
    return candidates;
  }

  /**
   * Visits local objects in heap order, stopping before expanding the remaining frontier when
   * the visitor rejects a candidate. Returned identifiers are objects excluded by that stop.
   */
  public List<String> visitBestFirstObjectCandidates(
      Map<Integer, AggregateRTree> indexes,
      QueryPoint queryPoint,
      Predicate<ObjectCandidate> visitor) {
    if (root == null) {
      return List.of();
    }
    PriorityQueue<NodeBound> heap = new PriorityQueue<>(
        Comparator.comparingDouble(NodeBound::upperBound).reversed());
    heap.add(new NodeBound(root, scoreUpperBound(root, indexes, queryPoint)));
    while (!heap.isEmpty()) {
      NodeBound current = heap.remove();
      Node node = current.node();
      if (node.leaf()) {
        for (int index = 0; index < node.objects().size(); index++) {
          ObjectEntry object = node.objects().get(index);
          if (!visitor.test(new ObjectCandidate(
              object.objectId(), object.instances(), current.upperBound()))) {
            List<String> skipped = new ArrayList<>();
            for (int remaining = index; remaining < node.objects().size(); remaining++) {
              skipped.add(node.objects().get(remaining).objectId());
            }
            for (NodeBound pending : heap) {
              skipped.addAll(pending.node().objectIds());
            }
            return skipped;
          }
        }
      } else {
        for (Node child : node.children()) {
          heap.add(new NodeBound(child, scoreUpperBound(child, indexes, queryPoint)));
        }
      }
    }
    return List.of();
  }

  private static double scoreUpperBound(
      Node candidateNode,
      Map<Integer, AggregateRTree> indexes,
      QueryPoint queryPoint) {
    return indexes.values().stream()
        .mapToDouble(index -> index.possiblyDominatedLeafMass(
            candidateNode.minimum(), candidateNode.maximum(), queryPoint))
        .sum();
  }

  private double possiblyDominatedLeafMass(
      double[] candidateMinimum, double[] candidateMaximum, QueryPoint queryPoint) {
    return root == null
        ? 0.0
        : possiblyDominatedLeafMass(candidateMinimum, candidateMaximum, root, queryPoint);
  }

  private double possiblyDominatedLeafMass(
      double[] candidateMinimum,
      double[] candidateMaximum,
      Node node,
      QueryPoint queryPoint) {
    if (!DominanceScorer.mbrCouldDynamicallyDominateMbr(
        candidateMinimum, candidateMaximum, node.minimum(), node.maximum(), queryPoint)) {
      return 0.0;
    }
    if (node.leaf()) {
      return node.probabilityMass();
    }
    return node.children().stream()
        .mapToDouble(child -> possiblyDominatedLeafMass(
            candidateMinimum, candidateMaximum, child, queryPoint))
        .sum();
  }

  private double exactDominatedMass(
      ProbabilisticInstance candidate, Node node, QueryPoint queryPoint) {
    if (DominanceScorer.dynamicallyDominatesMbrFully(
        candidate, node.minimum(), node.maximum(), queryPoint)) {
      double mass = node.probabilityMass();
      if (node.objectIds().contains(candidate.objectId())) {
        mass -= objectMassById.getOrDefault(candidate.objectId(), 0.0);
      }
      return mass;
    }
    if (!DominanceScorer.dynamicallyDominatesMbrPossibly(
        candidate, node.minimum(), node.maximum(), queryPoint)) {
      return 0.0;
    }
    if (node.leaf()) {
      double mass = 0.0;
      for (ObjectEntry object : node.objects()) {
        for (ProbabilisticInstance other : object.instances()) {
          if (!candidate.objectId().equals(other.objectId())
              && DominanceScorer.dynamicallyDominates(candidate, other, queryPoint)) {
            mass += other.probability();
          }
        }
      }
      return mass;
    }
    double mass = 0.0;
    for (Node child : node.children()) {
      mass += exactDominatedMass(candidate, child, queryPoint);
    }
    return mass;
  }

  private List<Node> exportedNodes(int requestedLevel) {
    if (root == null) {
      return List.of();
    }
    int level = Math.max(0, Math.min(requestedLevel, height()));
    List<Node> nodes = new ArrayList<>();
    collectLevel(root, level, nodes);
    return nodes;
  }

  private static void collectLevel(Node node, int level, List<Node> target) {
    if (node.level() == level || node.leaf()) {
      target.add(node);
      return;
    }
    for (Node child : node.children()) {
      collectLevel(child, level, target);
    }
  }

  private static boolean partiallyDominates(
      ProbabilisticInstance candidate, Node node, QueryPoint queryPoint) {
    return !DominanceScorer.dynamicallyDominatesMbrFully(
        candidate, node.minimum(), node.maximum(), queryPoint)
        && DominanceScorer.dynamicallyDominatesMbrPossibly(
            candidate, node.minimum(), node.maximum(), queryPoint);
  }

  private static String nodeId(int partitionId, int level, int serial) {
    return "p" + partitionId + "-l" + level + "-n" + serial;
  }

  private static double center(double[] minimum, double[] maximum, int dimension) {
    return (minimum[dimension] + maximum[dimension]) / 2.0;
  }

  /**
   * Packs 2D MBRs using Sort-Tile-Recursive grouping to avoid stripe-shaped parent rectangles.
   * Higher-dimensional inputs still use their first two spatial dimensions for index packing.
   */
  private static <T> List<List<T>> strGroups(
      List<T> entries,
      int capacity,
      Function<T, double[]> minimumAccessor,
      Function<T, double[]> maximumAccessor) {
    if (entries.isEmpty()) {
      return List.of();
    }
    int dimensions = minimumAccessor.apply(entries.get(0)).length;
    int yDimension = dimensions > 1 ? 1 : 0;
    int groupCount = (entries.size() + capacity - 1) / capacity;
    int slices = Math.max(1, (int) Math.ceil(Math.sqrt(groupCount)));
    int sliceCapacity = (entries.size() + slices - 1) / slices;
    List<T> ordered = new ArrayList<>(entries);
    ordered.sort(Comparator.comparingDouble(
        entry -> center(minimumAccessor.apply(entry), maximumAccessor.apply(entry), 0)));
    List<List<T>> groups = new ArrayList<>(groupCount);
    for (int sliceStart = 0; sliceStart < ordered.size(); sliceStart += sliceCapacity) {
      List<T> slice = new ArrayList<>(
          ordered.subList(sliceStart, Math.min(sliceStart + sliceCapacity, ordered.size())));
      slice.sort(Comparator.comparingDouble(
          entry -> center(minimumAccessor.apply(entry), maximumAccessor.apply(entry), yDimension)));
      for (int start = 0; start < slice.size(); start += capacity) {
        groups.add(new ArrayList<>(
            slice.subList(start, Math.min(start + capacity, slice.size()))));
      }
    }
    return groups;
  }

  private static double[] minimum(List<double[]> values) {
    double[] minimum = values.get(0).clone();
    for (double[] value : values) {
      for (int d = 0; d < minimum.length; d++) {
        minimum[d] = Math.min(minimum[d], value[d]);
      }
    }
    return minimum;
  }

  private static double[] maximum(List<double[]> values) {
    double[] maximum = values.get(0).clone();
    for (double[] value : values) {
      for (int d = 0; d < maximum.length; d++) {
        maximum[d] = Math.max(maximum[d], value[d]);
      }
    }
    return maximum;
  }

  public static final class LevelSelection implements Serializable {
    private final int partitionId;
    private final int level;
    private final int exportedNodes;
    private final long estimatedPartialReferences;
    private final long estimatedTraversalObjects;
    private final long estimatedCommunicationCost;

    LevelSelection(
        int partitionId,
        int level,
        int exportedNodes,
        long estimatedPartialReferences,
        long estimatedTraversalObjects,
        long estimatedCommunicationCost) {
      this.partitionId = partitionId;
      this.level = level;
      this.exportedNodes = exportedNodes;
      this.estimatedPartialReferences = estimatedPartialReferences;
      this.estimatedTraversalObjects = estimatedTraversalObjects;
      this.estimatedCommunicationCost = estimatedCommunicationCost;
    }

    public int partitionId() { return partitionId; }
    public int level() { return level; }
    public int exportedNodes() { return exportedNodes; }
    public long estimatedPartialReferences() { return estimatedPartialReferences; }
    public long estimatedTraversalObjects() { return estimatedTraversalObjects; }
    public long estimatedCommunicationCost() { return estimatedCommunicationCost; }
  }

  public static final class NodeReference implements Serializable {
    private final int partitionId;
    private final String nodeId;
    private final int level;
    private final double probabilityMass;

    NodeReference(int partitionId, String nodeId, int level, double probabilityMass) {
      this.partitionId = partitionId;
      this.nodeId = nodeId;
      this.level = level;
      this.probabilityMass = probabilityMass;
    }

    public int partitionId() { return partitionId; }
    public String nodeId() { return nodeId; }
    public int level() { return level; }
    public double probabilityMass() { return probabilityMass; }
  }

  public static final class Inspection implements Serializable {
    private final double fullyDominatedMass;
    private final List<NodeReference> partiallyDominatedNodes;

    Inspection(double fullyDominatedMass, List<NodeReference> partiallyDominatedNodes) {
      this.fullyDominatedMass = fullyDominatedMass;
      this.partiallyDominatedNodes = partiallyDominatedNodes;
    }

    public double fullyDominatedMass() { return fullyDominatedMass; }
    public List<NodeReference> partiallyDominatedNodes() { return partiallyDominatedNodes; }

    public double partialUpperMass() {
      return partiallyDominatedNodes.stream()
          .mapToDouble(NodeReference::probabilityMass)
          .sum();
    }
  }

  public static final class ObjectCandidate implements Serializable {
    private final String objectId;
    private final List<ProbabilisticInstance> instances;
    private final double traversalUpperBound;

    ObjectCandidate(
        String objectId,
        List<ProbabilisticInstance> instances,
        double traversalUpperBound) {
      this.objectId = objectId;
      this.instances = new ArrayList<>(instances);
      this.traversalUpperBound = traversalUpperBound;
    }

    public String objectId() { return objectId; }
    public List<ProbabilisticInstance> instances() { return instances; }
    public double traversalUpperBound() { return traversalUpperBound; }
  }

  private record NodeBound(Node node, double upperBound) implements Serializable {
  }

  private static final class ObjectEntry implements Serializable {
    private final String objectId;
    private final List<ProbabilisticInstance> instances;
    private final double[] minimum;
    private final double[] maximum;
    private final double probabilityMass;

    private ObjectEntry(
        String objectId,
        List<ProbabilisticInstance> instances,
        double[] minimum,
        double[] maximum,
        double probabilityMass) {
      this.objectId = objectId;
      this.instances = instances;
      this.minimum = minimum;
      this.maximum = maximum;
      this.probabilityMass = probabilityMass;
    }

    private String objectId() { return objectId; }
    private List<ProbabilisticInstance> instances() { return instances; }
    private double[] minimum() { return minimum; }
    private double[] maximum() { return maximum; }
    private double probabilityMass() { return probabilityMass; }

    private static ObjectEntry from(String objectId, List<ProbabilisticInstance> instances) {
      if (instances.stream().anyMatch(instance -> !instance.hasMbr())) {
        throw new IllegalArgumentException("aR-tree inputs must carry MBR bounds");
      }
      ProbabilisticInstance first = instances.get(0);
      double[] minimum = first.mbrMin();
      double[] maximum = first.mbrMax();
      for (ProbabilisticInstance instance : instances) {
        double[] instanceMinimum = instance.mbrMin();
        double[] instanceMaximum = instance.mbrMax();
        for (int d = 0; d < minimum.length; d++) {
          minimum[d] = Math.min(minimum[d], instanceMinimum[d]);
          maximum[d] = Math.max(maximum[d], instanceMaximum[d]);
        }
      }
      return new ObjectEntry(
          objectId,
          new ArrayList<>(instances),
          minimum,
          maximum,
          instances.stream().mapToDouble(ProbabilisticInstance::probability).sum());
    }
  }

  private static final class Node implements Serializable {
    private final String id;
    private final int partitionId;
    private final int level;
    private final List<Node> children;
    private final List<ObjectEntry> objects;
    private final List<String> objectIds;
    private final double[] minimum;
    private final double[] maximum;
    private final double probabilityMass;
    private final int objectCount;

    private Node(
        String id,
        int partitionId,
        int level,
        List<Node> children,
        List<ObjectEntry> objects,
        List<String> objectIds,
        double[] minimum,
        double[] maximum,
        double probabilityMass,
        int objectCount) {
      this.id = id;
      this.partitionId = partitionId;
      this.level = level;
      this.children = children;
      this.objects = objects;
      this.objectIds = objectIds;
      this.minimum = minimum;
      this.maximum = maximum;
      this.probabilityMass = probabilityMass;
      this.objectCount = objectCount;
    }

    private String id() { return id; }
    private int level() { return level; }
    private List<Node> children() { return children; }
    private List<ObjectEntry> objects() { return objects; }
    private List<String> objectIds() { return objectIds; }
    private double[] minimum() { return minimum; }
    private double[] maximum() { return maximum; }
    private double probabilityMass() { return probabilityMass; }
    private int objectCount() { return objectCount; }

    private static Node leaf(String id, int partitionId, List<ObjectEntry> objects) {
      return new Node(
          id,
          partitionId,
          0,
          new ArrayList<>(),
          new ArrayList<>(objects),
          objects.stream().map(ObjectEntry::objectId).collect(Collectors.toCollection(ArrayList::new)),
          AggregateRTree.minimum(objects.stream().map(ObjectEntry::minimum).toList()),
          AggregateRTree.maximum(objects.stream().map(ObjectEntry::maximum).toList()),
          objects.stream().mapToDouble(ObjectEntry::probabilityMass).sum(),
          objects.size());
    }

    private static Node branch(String id, int partitionId, int level, List<Node> children) {
      return new Node(
          id,
          partitionId,
          level,
          new ArrayList<>(children),
          new ArrayList<>(),
          children.stream()
              .flatMap(child -> child.objectIds().stream())
              .collect(Collectors.toCollection(ArrayList::new)),
          AggregateRTree.minimum(children.stream().map(Node::minimum).toList()),
          AggregateRTree.maximum(children.stream().map(Node::maximum).toList()),
          children.stream().mapToDouble(Node::probabilityMass).sum(),
          children.stream().mapToInt(Node::objectCount).sum());
    }

    private static Node summary(Node source, Map<String, Node> nodesById) {
      List<Node> children = source.children().stream()
          .map(child -> summary(child, nodesById))
          .toList();
      Node summary = new Node(
          source.id,
          source.partitionId,
          source.level,
          new ArrayList<>(children),
          new ArrayList<>(),
          new ArrayList<>(),
          source.minimum.clone(),
          source.maximum.clone(),
          source.probabilityMass,
          source.objectCount);
      nodesById.put(summary.id(), summary);
      return summary;
    }

    private boolean leaf() {
      return level == 0;
    }
  }
}
