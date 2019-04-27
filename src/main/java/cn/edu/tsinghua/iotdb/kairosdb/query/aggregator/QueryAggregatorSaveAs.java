package cn.edu.tsinghua.iotdb.kairosdb.query.aggregator;

import cn.edu.tsinghua.iotdb.kairosdb.dao.MetricsManager;
import cn.edu.tsinghua.iotdb.kairosdb.query.result.MetricResult;

public class QueryAggregatorSaveAs extends QueryAggregator {

  private String metricName;

  QueryAggregatorSaveAs() {
    super(QueryAggregatorType.SAVE_AS);
  }

  @Override
  public MetricResult doAggregate(MetricResult result) {
    MetricsManager.addDataPoints(result, metricName);
    return result;
  }

  public String getMetricName() {
    return metricName;
  }

  void setMetricName(String metricName) {
    this.metricName = metricName;
  }

}
