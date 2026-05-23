package com.thesis.topk.model;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

public class QueryPoint implements Serializable {
  private String queryId;
  private double[] coordinates;

  public QueryPoint() {
    this.queryId = "";
    this.coordinates = new double[0];
  }

  public QueryPoint(String queryId, double[] coordinates) {
    this.queryId = Objects.requireNonNull(queryId);
    this.coordinates = coordinates.clone();
  }

  public String queryId() {
    return queryId;
  }

  public double[] coordinates() {
    return coordinates.clone();
  }

  public String getQueryId() {
    return queryId;
  }

  public void setQueryId(String queryId) {
    this.queryId = queryId;
  }

  public double[] getCoordinates() {
    return coordinates;
  }

  public void setCoordinates(double[] coordinates) {
    this.coordinates = coordinates;
  }

  @Override
  public String toString() {
    return "QueryPoint{" +
        "queryId='" + queryId + '\'' +
        ", coordinates=" + Arrays.toString(coordinates) +
        '}';
  }
}
