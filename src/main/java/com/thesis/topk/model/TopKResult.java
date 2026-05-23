package com.thesis.topk.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class TopKResult implements Serializable {
  private String queryId;
  private long eventTime;
  private List<CandidateScore> scores;

  public TopKResult() {
    this.queryId = "";
    this.scores = new ArrayList<>();
  }

  public TopKResult(String queryId, long eventTime, List<CandidateScore> scores) {
    this.queryId = Objects.requireNonNull(queryId);
    this.eventTime = eventTime;
    this.scores = new ArrayList<>(scores);
  }

  public String queryId() {
    return queryId;
  }

  public long eventTime() {
    return eventTime;
  }

  public List<CandidateScore> scores() {
    return scores;
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

  public List<CandidateScore> getScores() {
    return scores;
  }

  public void setScores(List<CandidateScore> scores) {
    this.scores = scores;
  }

  @Override
  public String toString() {
    return "TopKResult{" +
        "queryId='" + queryId + '\'' +
        ", eventTime=" + eventTime +
        ", scores=" + scores +
        '}';
  }
}
