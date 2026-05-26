package com.thesis.topk.model;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

public class ProbabilisticInstance implements Serializable {
  private String objectId;
  private String queryId;
  private String instanceId;
  private long eventTime;
  private double probability;
  private int serverPartition;
  private double[] mbrMin;
  private double[] mbrMax;
  private double[] attributes;

  public ProbabilisticInstance() {
    this.objectId = "";
    this.queryId = "";
    this.instanceId = "";
    this.serverPartition = -1;
    this.mbrMin = new double[0];
    this.mbrMax = new double[0];
    this.attributes = new double[0];
  }

  public ProbabilisticInstance(String objectId, String queryId, String instanceId, long eventTime,
      double probability, double[] attributes) {
    this(objectId, queryId, instanceId, eventTime, probability, -1, attributes);
  }

  public ProbabilisticInstance(String objectId, String queryId, String instanceId, long eventTime,
      double probability, int serverPartition, double[] attributes) {
    this(objectId, queryId, instanceId, eventTime, probability, serverPartition, attributes,
        new double[0], new double[0]);
  }

  public ProbabilisticInstance(String objectId, String queryId, String instanceId, long eventTime,
      double probability, int serverPartition, double[] attributes, double[] mbrMin, double[] mbrMax) {
    if (probability < 0.0 || probability > 1.0) {
      throw new IllegalArgumentException("probability must be in [0, 1]");
    }
    this.objectId = Objects.requireNonNull(objectId);
    this.queryId = Objects.requireNonNull(queryId);
    this.instanceId = Objects.requireNonNull(instanceId);
    this.eventTime = eventTime;
    this.probability = probability;
    this.serverPartition = serverPartition;
    this.mbrMin = mbrMin.clone();
    this.mbrMax = mbrMax.clone();
    this.attributes = attributes.clone();
    if ((mbrMin.length != 0 || mbrMax.length != 0)
        && (mbrMin.length != attributes.length || mbrMax.length != attributes.length)) {
      throw new IllegalArgumentException("MBR coordinates must be empty or match attributes");
    }
  }

  public String objectId() {
    return objectId;
  }

  public String queryId() {
    return queryId;
  }

  public String instanceId() {
    return instanceId;
  }

  public long eventTime() {
    return eventTime;
  }

  public double probability() {
    return probability;
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

  public String getInstanceId() {
    return instanceId;
  }

  public void setInstanceId(String instanceId) {
    this.instanceId = instanceId;
  }

  public long getEventTime() {
    return eventTime;
  }

  public void setEventTime(long eventTime) {
    this.eventTime = eventTime;
  }

  public double getProbability() {
    return probability;
  }

  public void setProbability(double probability) {
    this.probability = probability;
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

  public ProbabilisticInstance withProbability(double newProbability) {
    return new ProbabilisticInstance(
        objectId, queryId, instanceId, eventTime, newProbability, serverPartition, attributes,
        mbrMin, mbrMax);
  }

  @Override
  public String toString() {
    return "ProbabilisticInstance{" +
        "objectId='" + objectId + '\'' +
        ", queryId='" + queryId + '\'' +
        ", instanceId='" + instanceId + '\'' +
        ", eventTime=" + eventTime +
        ", probability=" + probability +
        ", serverPartition=" + serverPartition +
        ", mbrMin=" + Arrays.toString(mbrMin) +
        ", mbrMax=" + Arrays.toString(mbrMax) +
        ", attributes=" + Arrays.toString(attributes) +
        '}';
  }
}
