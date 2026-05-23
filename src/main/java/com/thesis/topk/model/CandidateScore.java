package com.thesis.topk.model;

import java.io.Serializable;
import java.util.Objects;

public class CandidateScore implements Serializable, Comparable<CandidateScore> {
  private String objectId;
  private String queryId;
  private double exactScore;
  private double lowerBound;
  private double upperBound;
  private int instanceCount;

  public CandidateScore() {
    this.objectId = "";
    this.queryId = "";
  }

  public CandidateScore(String objectId, String queryId, double exactScore, double lowerBound,
      double upperBound, int instanceCount) {
    this.objectId = Objects.requireNonNull(objectId);
    this.queryId = Objects.requireNonNull(queryId);
    this.exactScore = exactScore;
    this.lowerBound = lowerBound;
    this.upperBound = upperBound;
    this.instanceCount = instanceCount;
  }

  public String objectId() {
    return objectId;
  }

  public String queryId() {
    return queryId;
  }

  public double exactScore() {
    return exactScore;
  }

  public double lowerBound() {
    return lowerBound;
  }

  public double upperBound() {
    return upperBound;
  }

  public int instanceCount() {
    return instanceCount;
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

  public double getExactScore() {
    return exactScore;
  }

  public void setExactScore(double exactScore) {
    this.exactScore = exactScore;
  }

  public double getLowerBound() {
    return lowerBound;
  }

  public void setLowerBound(double lowerBound) {
    this.lowerBound = lowerBound;
  }

  public double getUpperBound() {
    return upperBound;
  }

  public void setUpperBound(double upperBound) {
    this.upperBound = upperBound;
  }

  public int getInstanceCount() {
    return instanceCount;
  }

  public void setInstanceCount(int instanceCount) {
    this.instanceCount = instanceCount;
  }

  @Override
  public int compareTo(CandidateScore other) {
    int scoreCmp = Double.compare(other.exactScore, exactScore);
    if (scoreCmp != 0) {
      return scoreCmp;
    }
    return objectId.compareTo(other.objectId);
  }

  @Override
  public String toString() {
    return "CandidateScore{" +
        "objectId='" + objectId + '\'' +
        ", queryId='" + queryId + '\'' +
        ", exactScore=" + exactScore +
        ", lowerBound=" + lowerBound +
        ", upperBound=" + upperBound +
        ", instanceCount=" + instanceCount +
        '}';
  }
}
