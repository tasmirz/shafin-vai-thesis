package com.thesis.topk.hadoop;

import com.thesis.topk.algorithm.index.AggregateRTree;
import com.thesis.topk.model.ProbabilisticInstance;
import com.thesis.topk.model.QueryPoint;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Base64;
import java.util.List;
import java.util.Map;

/**
 * Serialization utilities for Rai-Lian aR-tree baseline inter-job communication.
 * Uses Java serialization with Base64 encoding for Hadoop Text compatibility.
 */
public final class RaiLianSerde {
  private RaiLianSerde() {
  }

  public static String serialize(Object object) {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
         ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(object);
      return Base64.getEncoder().encodeToString(baos.toByteArray());
    } catch (IOException e) {
      throw new IllegalStateException("Failed to serialize object", e);
    }
  }

  public static <T> T deserialize(String base64, Class<T> clazz) {
    try (ByteArrayInputStream bais = new ByteArrayInputStream(Base64.getDecoder().decode(base64));
         ObjectInputStream ois = new ObjectInputStream(bais)) {
      @SuppressWarnings("unchecked")
      T result = (T) ois.readObject();
      return result;
    } catch (IOException | ClassNotFoundException e) {
      throw new IllegalStateException("Failed to deserialize object", e);
    }
  }

  public static String serializeIndex(AggregateRTree index) {
    return serialize(index.summaryOnly());
  }

  public static AggregateRTree deserializeIndex(String base64) {
    return deserialize(base64, AggregateRTree.class);
  }

  public static String serializeInspection(AggregateRTree.Inspection inspection) {
    return serialize(inspection);
  }

  public static AggregateRTree.Inspection deserializeInspection(String base64) {
    return deserialize(base64, AggregateRTree.Inspection.class);
  }

  public static String serializeLevelSelection(AggregateRTree.LevelSelection selection) {
    return serialize(selection);
  }

  public static AggregateRTree.LevelSelection deserializeLevelSelection(String base64) {
    return deserialize(base64, AggregateRTree.LevelSelection.class);
  }

  public static String serializeQueryPoint(QueryPoint queryPoint) {
    return serialize(queryPoint);
  }

  public static QueryPoint deserializeQueryPoint(String base64) {
    return deserialize(base64, QueryPoint.class);
  }

  public static String serializeInstances(List<ProbabilisticInstance> instances) {
    return serialize(instances);
  }

  @SuppressWarnings("unchecked")
  public static List<ProbabilisticInstance> deserializeInstances(String base64) {
    return (List<ProbabilisticInstance>) deserialize(base64, List.class);
  }

  public static String serializeLevelMap(Map<Integer, Integer> levelMap) {
    return serialize(levelMap);
  }

  @SuppressWarnings("unchecked")
  public static Map<Integer, Integer> deserializeLevelMap(String base64) {
    return (Map<Integer, Integer>) deserialize(base64, Map.class);
  }

  public static String serializeThresholds(Map<Integer, Double> thresholds) {
    return serialize(thresholds);
  }

  @SuppressWarnings("unchecked")
  public static Map<Integer, Double> deserializeThresholds(String base64) {
    return (Map<Integer, Double>) deserialize(base64, Map.class);
  }
}