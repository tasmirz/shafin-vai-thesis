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
  private double[] attributes;

  public ProbabilisticInstance() {
    this.objectId = "";
    this.queryId = "";
    this.instanceId = "";
    this.attributes = new double[0];
  }

  public ProbabilisticInstance(String objectId, String queryId, String instanceId, long eventTime,
      double probability, double[] attributes) {
    if (probability < 0.0 || probability > 1.0) {
      throw new IllegalArgumentException("probability must be in [0, 1]");
    }
    this.objectId = Objects.requireNonNull(objectId);
    this.queryId = Objects.requireNonNull(queryId);
    this.instanceId = Objects.requireNonNull(instanceId);
    this.eventTime = eventTime;
    this.probability = probability;
    this.attributes = attributes.clone();
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

  public double[] getAttributes() {
    return attributes;
  }

  public void setAttributes(double[] attributes) {
    this.attributes = attributes;
  }

  public ProbabilisticInstance withProbability(double newProbability) {
    return new ProbabilisticInstance(objectId, queryId, instanceId, eventTime, newProbability, attributes);
  }

  @Override
  public String toString() {
    return "ProbabilisticInstance{" +
        "objectId='" + objectId + '\'' +
        ", queryId='" + queryId + '\'' +
        ", instanceId='" + instanceId + '\'' +
        ", eventTime=" + eventTime +
        ", probability=" + probability +
        ", attributes=" + Arrays.toString(attributes) +
        '}';
  }
}
