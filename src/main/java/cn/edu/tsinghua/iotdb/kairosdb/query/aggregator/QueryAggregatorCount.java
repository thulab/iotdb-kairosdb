package cn.edu.tsinghua.iotdb.kairosdb.query.aggregator;

import cn.edu.tsinghua.iotdb.kairosdb.datastore.Duration;
import cn.edu.tsinghua.iotdb.kairosdb.query.QueryException;
import cn.edu.tsinghua.iotdb.kairosdb.query.result.MetricResult;
import cn.edu.tsinghua.iotdb.kairosdb.query.result.MetricValueResult;
import cn.edu.tsinghua.iotdb.kairosdb.query.result.QueryDataPoint;
import java.util.List;

public class QueryAggregatorCount extends QueryAggregator
    implements QueryAggregatorSampling, QueryAggregatorAlignable {

  private Duration sampling;

  private QueryAggregatorAlign align;

  private long startTimestamp;
  private long endTimestamp;

  QueryAggregatorCount() {
    super(QueryAggregatorType.COUNT);
  }

  @Override
  public MetricResult doAggregate(MetricResult result) throws QueryException {
    return useMethodAggregate(this, result);
  }

  @Override
  public MetricValueResult aggregate(MetricValueResult valueResult) {
    MetricValueResult newValueResult = new MetricValueResult(valueResult.getName());

    long step = getSampling().toTimestamp();

    List<List<QueryDataPoint>> splitPoints =
        valueResult.splitDataPoint(getStartTimestamp(), step, getAlign());

    for (List<QueryDataPoint> points : splitPoints) {
      long timestamp = computeTimestampByAlign(this, points.get(0).getTimestamp(), step);
      newValueResult.addDataPoint(new QueryDataPoint(timestamp, points.size()));
    }

    return newValueResult;
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
  public void setStartTimestamp(long startTimestamp) {
    this.startTimestamp = startTimestamp;
  }

  @Override
  public long getStartTimestamp() {
    return this.startTimestamp;
  }

  @Override
  public void setEndTimestamp(long endTimestamp) {
    this.endTimestamp = endTimestamp;
  }

  @Override
  public long getEndTimestamp() {
    return this.endTimestamp;
  }

}
