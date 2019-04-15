package cn.edu.tsinghua.iotdb.kairosdb.query.aggregator;

import cn.edu.tsinghua.iotdb.kairosdb.query.result.MetricResult;

public abstract class QueryAggregator {

  private final QueryAggregatorType type;

  protected QueryAggregator(QueryAggregatorType type) {
    this.type = type;
  }

  public QueryAggregatorType getType() {
    return type;
  }

  public abstract MetricResult doAggregate(MetricResult result);

}
