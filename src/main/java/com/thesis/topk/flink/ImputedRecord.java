package com.thesis.topk.flink;

import com.thesis.topk.model.ProbabilisticInstance;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class ImputedRecord implements Serializable {
  private String queryId;
  private long eventTime;
  private List<ProbabilisticInstance> instances;

  public ImputedRecord() {
    this.queryId = "";
    this.instances = new ArrayList<>();
  }

  public ImputedRecord(String queryId, long eventTime, List<ProbabilisticInstance> instances) {
    this.queryId = queryId;
    this.eventTime = eventTime;
    this.instances = new ArrayList<>(instances);
  }

  public String queryId() {
    return queryId;
  }

  public long eventTime() {
    return eventTime;
  }

  public List<ProbabilisticInstance> instances() {
    return instances;
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

  public List<ProbabilisticInstance> getInstances() {
    return instances;
  }

  public void setInstances(List<ProbabilisticInstance> instances) {
    this.instances = instances;
  }
}
