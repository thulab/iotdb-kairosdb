package cn.edu.tsinghua.iotdb.kairosdb.query.aggregator;

import cn.edu.tsinghua.iotdb.kairosdb.datastore.TimeUnit;
import cn.edu.tsinghua.iotdb.kairosdb.query.result.MetricResult;

public class QueryAggregatorRate extends QueryAggregator {

  /**
   * example:
   *    {
   *      "name": "rate",
   *      "sampling": {
   *        "unit": "minutes",
   *        "value": 1
   *      }
   *    }
   */

  private TimeUnit unit;

  protected QueryAggregatorRate() {
    super(QueryAggregatorType.RATE);
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
