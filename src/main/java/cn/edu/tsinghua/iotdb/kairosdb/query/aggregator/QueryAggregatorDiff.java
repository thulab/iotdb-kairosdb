package cn.edu.tsinghua.iotdb.kairosdb.query.aggregator;

import cn.edu.tsinghua.iotdb.kairosdb.query.result.MetricResult;

public class QueryAggregatorDiff extends QueryAggregator {

  protected QueryAggregatorDiff() {
    super(QueryAggregatorType.DIFF);
  }

  @Override
  public MetricResult doAggregate(MetricResult result) {
    return null;
  }
}
