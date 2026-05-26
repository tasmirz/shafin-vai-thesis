package com.thesis.topk.io;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.thesis.topk.model.OpType;
import com.thesis.topk.model.RawEvent;
import java.io.IOException;

public final class RawEventJsonSerde {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private RawEventJsonSerde() {
  }

  public static String toJson(RawEvent event) {
    ObjectNode root = MAPPER.createObjectNode();
    root.put("objectId", event.objectId());
    root.put("instanceId", event.instanceId());
    root.put("queryId", event.queryId());
    root.put("eventTime", event.eventTime());
    root.put("appearanceProbability", event.appearanceProbability());
    root.put("serverPartition", event.serverPartition());
    root.put("opType", event.opType().name());

    double[] attributes = event.attributes();
    boolean[] missingMask = event.missingMask();
    ArrayNode attrs = root.putArray("attributes");
    ArrayNode missing = root.putArray("missingMask");
    for (int i = 0; i < attributes.length; i++) {
      if (missingMask[i] || Double.isNaN(attributes[i])) {
        attrs.addNull();
      } else {
        attrs.add(attributes[i]);
      }
      missing.add(missingMask[i]);
    }
    ArrayNode mbrMin = root.putArray("mbrMin");
    ArrayNode mbrMax = root.putArray("mbrMax");
    for (double value : event.mbrMin()) {
      mbrMin.add(value);
    }
    for (double value : event.mbrMax()) {
      mbrMax.add(value);
    }
    return root.toString();
  }

  public static RawEvent fromJson(String json) {
    try {
      JsonNode root = MAPPER.readTree(json);
      JsonNode attrNode = root.required("attributes");
      JsonNode missingNode = root.required("missingMask");
      if (!attrNode.isArray() || !missingNode.isArray() || attrNode.size() != missingNode.size()) {
        throw new IllegalArgumentException("attributes and missingMask must be arrays of equal size");
      }

      double[] attributes = new double[attrNode.size()];
      boolean[] missingMask = new boolean[missingNode.size()];
      for (int i = 0; i < attrNode.size(); i++) {
        missingMask[i] = missingNode.get(i).asBoolean();
        JsonNode value = attrNode.get(i);
        attributes[i] = value == null || value.isNull() ? Double.NaN : value.asDouble();
      }
      double[] mbrMin = array(root.path("mbrMin"));
      double[] mbrMax = array(root.path("mbrMax"));

      return new RawEvent(
          root.required("objectId").asText(),
          root.path("instanceId").asText(root.required("objectId").asText() + "#raw"),
          root.required("queryId").asText(),
          root.required("eventTime").asLong(),
          root.path("appearanceProbability").asDouble(1.0),
          root.path("serverPartition").asInt(-1),
          attributes,
          missingMask,
          mbrMin,
          mbrMax,
          OpType.valueOf(root.path("opType").asText(OpType.UPSERT.name())));
    } catch (IOException e) {
      throw new IllegalArgumentException("Invalid RawEvent JSON", e);
    }
  }

  private static double[] array(JsonNode node) {
    if (!node.isArray()) {
      return new double[0];
    }
    double[] values = new double[node.size()];
    for (int i = 0; i < node.size(); i++) {
      values[i] = node.get(i).asDouble();
    }
    return values;
  }
}
