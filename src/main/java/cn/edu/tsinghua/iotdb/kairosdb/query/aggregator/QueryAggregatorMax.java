package cn.edu.tsinghua.iotdb.kairosdb.query.aggregator;

import cn.edu.tsinghua.iotdb.kairosdb.datastore.Duration;
import cn.edu.tsinghua.iotdb.kairosdb.query.result.MetricResult;

public class QueryAggregatorMax extends QueryAggregator
    implements QueryAggregatorSampling, QueryAggregatorAlignable {

  private Duration sampling;

  private QueryAggregatorAlign align;

  protected QueryAggregatorMax() {
    super(QueryAggregatorType.MAX);
  }

  @Override
  public void setSampling(Duration sampling) {
    this.sampling = sampling;
  }

  @Override
  public Duration getSampling() {
    return sampling;
  }

  @Override
  public void setAlign(QueryAggregatorAlign align) {
    this.align = align;
  }

  @Override
  public QueryAggregatorAlign getAlign() {
    return align;
  }

  @Override
  public MetricResult doAggregate(MetricResult result) {
    return result;
  }

}
