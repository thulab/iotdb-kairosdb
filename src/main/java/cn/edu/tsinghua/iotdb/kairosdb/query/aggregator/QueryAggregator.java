package cn.edu.tsinghua.iotdb.kairosdb.query.aggregator;

import cn.edu.tsinghua.iotdb.kairosdb.query.QueryException;
import cn.edu.tsinghua.iotdb.kairosdb.query.result.MetricResult;
import cn.edu.tsinghua.iotdb.kairosdb.query.result.MetricValueResult;
import java.util.LinkedList;
import java.util.List;

public abstract class QueryAggregator {

  private final QueryAggregatorType type;

  protected QueryAggregator(QueryAggregatorType type) {
    this.type = type;
  }

  public QueryAggregatorType getType() {
    return type;
  }

  public abstract MetricResult doAggregate(MetricResult result) throws QueryException;

  static MetricResult useMethodAggregate(QueryAggregatorAlignable aggregator, MetricResult result)
      throws QueryException {
    List<MetricValueResult> valueResults = result.getResults();

    List<MetricValueResult> newValueResults = new LinkedList<>();

    for (MetricValueResult valueResult : valueResults) {

      if (valueResult.isTextType()) {
        continue;
      }

      MetricValueResult newValueResult = aggregator.aggregate(valueResult);
      newValueResult.setTags(valueResult.getTags());
      newValueResult.setGroupBy(valueResult.getGroupBy());

      newValueResults.add(newValueResult);

    }

    result.setResults(newValueResults);

    return result;
  }

  static long computeTimestampByAlign(QueryAggregatorAlignable aggregator, long timestamp, long step) {
    switch (aggregator.getAlign()) {
      case ALIGN_START_TIME:
        return ((timestamp - 1) / step) * step + aggregator.getStartTimestamp();
      case ALIGN_END_TIME:
        return ((timestamp + step - 1) / step) * step + aggregator.getStartTimestamp();
      default:
        return timestamp;
    }
  }

}
