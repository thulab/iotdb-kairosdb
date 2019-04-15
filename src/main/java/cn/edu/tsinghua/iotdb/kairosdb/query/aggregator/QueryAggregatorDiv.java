package cn.edu.tsinghua.iotdb.kairosdb.query.aggregator;

import cn.edu.tsinghua.iotdb.kairosdb.query.result.MetricResult;

public class QueryAggregatorDiv extends QueryAggregator {

  private double divisor;

  protected QueryAggregatorDiv() {
    super(QueryAggregatorType.DIV);
  }

  @Override
  public MetricResult doAggregate(MetricResult result) {
    return null;
  }

  public double getDivisor() {
    return divisor;
  }

  public void setDivisor(double divisor) {
    this.divisor = divisor;
  }

}
