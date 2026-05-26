package com.thesis.topk.model;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

public class RawEvent implements Serializable {
  private String objectId;
  private String instanceId;
  private String queryId;
  private long eventTime;
  private double appearanceProbability;
  private int serverPartition;
  private double[] mbrMin;
  private double[] mbrMax;
  private double[] attributes;
  private boolean[] missingMask;
  private OpType opType;

  public RawEvent() {
    this.objectId = "";
    this.instanceId = "";
    this.queryId = "";
    this.appearanceProbability = 1.0;
    this.serverPartition = -1;
    this.mbrMin = new double[0];
    this.mbrMax = new double[0];
    this.attributes = new double[0];
    this.missingMask = new boolean[0];
    this.opType = OpType.UPSERT;
  }

  public RawEvent(String objectId, String queryId, long eventTime, double[] attributes,
      boolean[] missingMask, OpType opType) {
    this(objectId, objectId + "#raw", queryId, eventTime, 1.0, -1, attributes, missingMask, opType);
  }

  public RawEvent(String objectId, String instanceId, String queryId, long eventTime,
      double appearanceProbability, int serverPartition, double[] attributes,
      boolean[] missingMask, OpType opType) {
    this(objectId, instanceId, queryId, eventTime, appearanceProbability, serverPartition,
        attributes, missingMask, new double[0], new double[0], opType);
  }

  public RawEvent(String objectId, String instanceId, String queryId, long eventTime,
      double appearanceProbability, int serverPartition, double[] attributes,
      boolean[] missingMask, double[] mbrMin, double[] mbrMax, OpType opType) {
    if (appearanceProbability < 0.0 || appearanceProbability > 1.0) {
      throw new IllegalArgumentException("appearanceProbability must be in [0, 1]");
    }
    this.objectId = Objects.requireNonNull(objectId);
    this.instanceId = Objects.requireNonNull(instanceId);
    this.queryId = Objects.requireNonNull(queryId);
    this.eventTime = eventTime;
    this.appearanceProbability = appearanceProbability;
    this.serverPartition = serverPartition;
    this.mbrMin = mbrMin.clone();
    this.mbrMax = mbrMax.clone();
    this.attributes = attributes.clone();
    this.missingMask = missingMask.clone();
    this.opType = Objects.requireNonNull(opType);
    if (attributes.length != missingMask.length) {
      throw new IllegalArgumentException("attributes and missingMask must have the same length");
    }
    if ((mbrMin.length != 0 || mbrMax.length != 0)
        && (mbrMin.length != attributes.length || mbrMax.length != attributes.length)) {
      throw new IllegalArgumentException("MBR coordinates must be empty or match attributes");
    }
  }

  public String objectId() {
    return objectId;
  }

  public String instanceId() {
    return instanceId;
  }

  public String queryId() {
    return queryId;
  }

  public long eventTime() {
    return eventTime;
  }

  public double appearanceProbability() {
    return appearanceProbability;
  }

  public int serverPartition() {
    return serverPartition;
  }

  public double[] mbrMin() {
    return mbrMin.clone();
  }

  public double[] mbrMax() {
    return mbrMax.clone();
  }

  public boolean hasMbr() {
    return mbrMin.length > 0
        && mbrMin.length == attributes.length
        && mbrMax.length == attributes.length;
  }

  public double[] attributes() {
    return attributes.clone();
  }

  public boolean[] missingMask() {
    return missingMask.clone();
  }

  public OpType opType() {
    return opType;
  }

  public String getObjectId() {
    return objectId;
  }

  public void setObjectId(String objectId) {
    this.objectId = objectId;
  }

  public String getInstanceId() {
    return instanceId;
  }

  public void setInstanceId(String instanceId) {
    this.instanceId = instanceId;
  }

  public String getQueryId() {
    return queryId;
  }

  public void setQueryId(String queryId) {
    this.queryId = queryId;
  }

  public long getEventTime() {
    return eventTime;
  }

  public void setEventTime(long eventTime) {
    this.eventTime = eventTime;
  }

  public double getAppearanceProbability() {
    return appearanceProbability;
  }

  public void setAppearanceProbability(double appearanceProbability) {
    this.appearanceProbability = appearanceProbability;
  }

  public int getServerPartition() {
    return serverPartition;
  }

  public void setServerPartition(int serverPartition) {
    this.serverPartition = serverPartition;
  }

  public double[] getMbrMin() {
    return mbrMin;
  }

  public void setMbrMin(double[] mbrMin) {
    this.mbrMin = mbrMin;
  }

  public double[] getMbrMax() {
    return mbrMax;
  }

  public void setMbrMax(double[] mbrMax) {
    this.mbrMax = mbrMax;
  }

  public double[] getAttributes() {
    return attributes;
  }

  public void setAttributes(double[] attributes) {
    this.attributes = attributes;
  }

  public boolean[] getMissingMask() {
    return missingMask;
  }

  public void setMissingMask(boolean[] missingMask) {
    this.missingMask = missingMask;
  }

  public OpType getOpType() {
    return opType;
  }

  public void setOpType(OpType opType) {
    this.opType = opType;
  }

  public boolean hasMissingAttributes() {
    for (boolean missing : missingMask) {
      if (missing) {
        return true;
      }
    }
    return false;
  }

  @Override
  public String toString() {
    return "RawEvent{" +
        "objectId='" + objectId + '\'' +
        ", instanceId='" + instanceId + '\'' +
        ", queryId='" + queryId + '\'' +
        ", eventTime=" + eventTime +
        ", appearanceProbability=" + appearanceProbability +
        ", serverPartition=" + serverPartition +
        ", mbrMin=" + Arrays.toString(mbrMin) +
        ", mbrMax=" + Arrays.toString(mbrMax) +
        ", attributes=" + Arrays.toString(attributes) +
        ", missingMask=" + Arrays.toString(missingMask) +
        ", opType=" + opType +
        '}';
  }
}
