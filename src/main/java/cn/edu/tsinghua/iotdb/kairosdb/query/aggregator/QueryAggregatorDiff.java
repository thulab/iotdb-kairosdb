package cn.edu.tsinghua.iotdb.kairosdb.query.aggregator;

import cn.edu.tsinghua.iotdb.kairosdb.query.result.MetricResult;
import cn.edu.tsinghua.iotdb.kairosdb.query.result.MetricValueResult;
import cn.edu.tsinghua.iotdb.kairosdb.query.result.QueryDataPoint;
import java.util.LinkedList;
import java.util.List;

public class QueryAggregatorDiff extends QueryAggregator {

  QueryAggregatorDiff() {
    super(QueryAggregatorType.DIFF);
  }

  @Override
  public MetricResult doAggregate(MetricResult result) {

    List<MetricValueResult> valueResults = result.getResults();

    for (MetricValueResult valueResult : valueResults) {

      List<QueryDataPoint> points = valueResult.getDatapoints();

      if (valueResult.isTextType() || points.isEmpty()) {
        continue;
      }

      List<QueryDataPoint> newPoints = new LinkedList<>();

      QueryDataPoint tmpPoint = points.get(0);
      points.remove(tmpPoint);

      for (QueryDataPoint point : points) {
        newPoints.add(new QueryDataPoint(point.getTimestamp(),
            point.getAsDouble() - tmpPoint.getAsDouble()));

        tmpPoint = point;
      }


      valueResult.setValues(newPoints);

    }

    return result;
  }

}
