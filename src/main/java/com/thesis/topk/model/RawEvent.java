package com.thesis.topk.model;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

public class RawEvent implements Serializable {
  private String objectId;
  private String queryId;
  private long eventTime;
  private double[] attributes;
  private boolean[] missingMask;
  private OpType opType;

  public RawEvent() {
    this.objectId = "";
    this.queryId = "";
    this.attributes = new double[0];
    this.missingMask = new boolean[0];
    this.opType = OpType.UPSERT;
  }

  public RawEvent(String objectId, String queryId, long eventTime, double[] attributes,
      boolean[] missingMask, OpType opType) {
    this.objectId = Objects.requireNonNull(objectId);
    this.queryId = Objects.requireNonNull(queryId);
    this.eventTime = eventTime;
    this.attributes = attributes.clone();
    this.missingMask = missingMask.clone();
    this.opType = Objects.requireNonNull(opType);
    if (attributes.length != missingMask.length) {
      throw new IllegalArgumentException("attributes and missingMask must have the same length");
    }
  }

  public String objectId() {
    return objectId;
  }

  public String queryId() {
    return queryId;
  }

  public long eventTime() {
    return eventTime;
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
        ", queryId='" + queryId + '\'' +
        ", eventTime=" + eventTime +
        ", attributes=" + Arrays.toString(attributes) +
        ", missingMask=" + Arrays.toString(missingMask) +
        ", opType=" + opType +
        '}';
  }
}
