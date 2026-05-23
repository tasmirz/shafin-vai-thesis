package com.thesis.topk.flink;

import com.thesis.topk.model.OpType;
import com.thesis.topk.model.ProbabilisticInstance;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class ImputedRecord implements Serializable {
  private String objectId;
  private String queryId;
  private long eventTime;
  private OpType opType;
  private List<ProbabilisticInstance> instances;

  public ImputedRecord() {
    this.objectId = "";
    this.queryId = "";
    this.opType = OpType.UPSERT;
    this.instances = new ArrayList<>();
  }

  public ImputedRecord(
      String objectId,
      String queryId,
      long eventTime,
      OpType opType,
      List<ProbabilisticInstance> instances) {
    this.objectId = objectId;
    this.queryId = queryId;
    this.eventTime = eventTime;
    this.opType = opType;
    this.instances = new ArrayList<>(instances);
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

  public List<ProbabilisticInstance> instances() {
    return instances;
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

  public OpType getOpType() {
    return opType;
  }

  public void setOpType(OpType opType) {
    this.opType = opType;
  }

  public List<ProbabilisticInstance> getInstances() {
    return instances;
  }

  public void setInstances(List<ProbabilisticInstance> instances) {
    this.instances = instances;
  }
}
