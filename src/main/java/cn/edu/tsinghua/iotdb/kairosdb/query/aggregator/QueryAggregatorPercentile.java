package cn.edu.tsinghua.iotdb.kairosdb.query.aggregator;

import cn.edu.tsinghua.iotdb.kairosdb.datastore.Duration;
import cn.edu.tsinghua.iotdb.kairosdb.query.result.MetricResult;

public class QueryAggregatorPercentile extends QueryAggregator implements QueryAggregatorSampling {

  private Duration sampling;

  private float percentile;

  protected QueryAggregatorPercentile() {
    super(QueryAggregatorType.PERCENTILE);
  }

  @Override
  public MetricResult doAggregate(MetricResult result) {
    return null;
  }

  @Override
  public void setSampling(Duration sampling) {
    this.sampling = sampling;
  }

  @Override
  public Duration getSampling() {
    return sampling;
  }

  public void setPercentile(float percentile) {
    this.percentile = percentile;
  }

  public float getPercentile() {
    return percentile;
  }
}
