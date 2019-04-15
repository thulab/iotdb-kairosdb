package cn.edu.tsinghua.iotdb.kairosdb.query.aggregator;

import cn.edu.tsinghua.iotdb.kairosdb.query.result.MetricResult;

public class QueryAggregatorSaveAs extends QueryAggregator {

  private String metricName;

  protected QueryAggregatorSaveAs() {
    super(QueryAggregatorType.SAVE_AS);
  }

  @Override
  public MetricResult doAggregate(MetricResult result) {
    return null;
  }

  public String getMetricName() {
    return metricName;
  }

  public void setMetricName(String metricName) {
    this.metricName = metricName;
  }

}
