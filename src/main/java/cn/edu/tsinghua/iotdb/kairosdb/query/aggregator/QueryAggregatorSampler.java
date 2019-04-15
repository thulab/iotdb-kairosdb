package cn.edu.tsinghua.iotdb.kairosdb.query.aggregator;

import cn.edu.tsinghua.iotdb.kairosdb.datastore.TimeUnit;
import cn.edu.tsinghua.iotdb.kairosdb.query.result.MetricResult;

public class QueryAggregatorSampler extends QueryAggregator {

  /**
   * example:
   *    {
   *      "name": "sampler",
   *      "unit": "milliseconds"
   *    }
   */

  private TimeUnit unit;

  protected QueryAggregatorSampler() {
    super(QueryAggregatorType.SAMPLER);
  }

  @Override
  public MetricResult doAggregate(MetricResult result) {
    return null;
  }

  public void setUnit(TimeUnit unit) {
    this.unit = unit;
  }

  public void setUnit(String unitStr) {
    this.unit = TimeUnit.from(unitStr);
  }

}
