package cn.edu.tsinghua.iotdb.kairosdb.query;

import cn.edu.tsinghua.iotdb.kairosdb.tsdb.DBWrapper;
import java.util.List;

public class QueryContext {

  private QueryMetric metric;
  private long segmentStartTime;
  private long segmentEndTime;
  private int timeSegmentIndex;
  private int metricCount;
  private List<List<List<DBWrapper>>> connections;

  public QueryMetric getMetric() {
    return metric;
  }

  public void setMetric(QueryMetric metric) {
    this.metric = metric;
  }

  public long getSegmentStartTime() {
    return segmentStartTime;
  }

  public void setSegmentStartTime(long segmentStartTime) {
    this.segmentStartTime = segmentStartTime;
  }

  public long getSegmentEndTime() {
    return segmentEndTime;
  }

  public void setSegmentEndTime(long segmentEndTime) {
    this.segmentEndTime = segmentEndTime;
  }

  public int getTimeSegmentIndex() {
    return timeSegmentIndex;
  }

  public void setTimeSegmentIndex(int timeSegmentIndex) {
    this.timeSegmentIndex = timeSegmentIndex;
  }

  public int getMetricCount() {
    return metricCount;
  }

  public void setMetricCount(int metricCount) {
    this.metricCount = metricCount;
  }

  public List<List<List<DBWrapper>>> getConnections() {
    return connections;
  }

  public void setConnections(
      List<List<List<DBWrapper>>> connections) {
    this.connections = connections;
  }

}
