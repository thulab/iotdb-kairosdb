package cn.edu.tsinghua.iotdb.kairosdb.query.aggregator;

import cn.edu.tsinghua.iotdb.kairosdb.datastore.TimeUnit;
import cn.edu.tsinghua.iotdb.kairosdb.query.result.MetricResult;
import cn.edu.tsinghua.iotdb.kairosdb.query.result.MetricValueResult;
import cn.edu.tsinghua.iotdb.kairosdb.query.result.QueryDataPoint;
import java.util.LinkedList;
import java.util.List;

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

  QueryAggregatorRate() {
    super(QueryAggregatorType.RATE);
  }

  @Override
  public MetricResult doAggregate(MetricResult result) {
    List<MetricValueResult> valueResults = result.getResults();

    List<MetricValueResult> newValueResults = new LinkedList<>();

    for (MetricValueResult valueResult : valueResults) {

      List<QueryDataPoint> newPoints = new LinkedList<>();

      List<QueryDataPoint> points = valueResult.getDatapoints();

      if (valueResult.isTextType() || points.isEmpty()) {
        continue;
      }

      QueryDataPoint tmpPoint = points.get(0);
      points.remove(tmpPoint);

      for (QueryDataPoint point : points) {
        long preTimestamp = tmpPoint.getTimestamp();
        long postTimestamp = point.getTimestamp();

        double rate = TimeUnit.getUnitTime(unit) / (double) (postTimestamp - preTimestamp);

        newPoints.add(new QueryDataPoint(postTimestamp,
            (tmpPoint.getAsDouble() - point.getAsDouble()) * rate));

        tmpPoint = point;
      }


      valueResult.setValues(newPoints);

    }

    result.setResults(newValueResults);

    return result;
  }

  public void setUnit(String unitStr) {
    this.unit = TimeUnit.from(unitStr);
  }

}
