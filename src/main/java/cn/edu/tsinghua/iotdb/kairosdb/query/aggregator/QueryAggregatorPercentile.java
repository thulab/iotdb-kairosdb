package cn.edu.tsinghua.iotdb.kairosdb.query.aggregator;

import cn.edu.tsinghua.iotdb.kairosdb.datastore.Duration;
import cn.edu.tsinghua.iotdb.kairosdb.query.QueryException;
import cn.edu.tsinghua.iotdb.kairosdb.query.result.MetricResult;
import cn.edu.tsinghua.iotdb.kairosdb.query.result.MetricValueResult;
import cn.edu.tsinghua.iotdb.kairosdb.query.result.QueryDataPoint;
import java.util.List;

public class QueryAggregatorPercentile extends QueryAggregator implements QueryAggregatorSampling, QueryAggregatorAlignable {

  private Duration sampling;

  private float percentile;

  private long startTimestamp;
  private long endTimestamp;

  QueryAggregatorPercentile() {
    super(QueryAggregatorType.PERCENTILE);
  }

  @Override
  public MetricResult doAggregate(MetricResult result) throws QueryException {
    return useMethodAggregate(this, result);
  }

  @Override
  public MetricValueResult aggregate(MetricValueResult valueResult) throws QueryException {
    MetricValueResult newValueResult = new MetricValueResult(valueResult.getName());

    if (getPercentile() <= 0 || getPercentile() > 1) {
      throw new QueryException("Among percentile aggregator, percentile must be in (0,1].");
    }

    long step = getSampling().toTimestamp();

    List<List<QueryDataPoint>> splitPoints = valueResult.splitDataPoint(getStartTimestamp(), step);

    for (List<QueryDataPoint> points : splitPoints) {

      newValueResult.addDataPoint(points.get((int) (points.size() * getPercentile())));

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

  void setPercentile(float percentile) {
    this.percentile = percentile;
  }

  private float getPercentile() {
    return percentile;
  }

  @Override
  public void setAlign(QueryAggregatorAlign align) {
    throw new UnsupportedOperationException();
  }

  @Override
  public QueryAggregatorAlign getAlign() {
    return null;
  }

  @Override
  public void setStartTimestamp(long startTimestamp) {
    this.startTimestamp = startTimestamp;
  }

  @Override
  public long getStartTimestamp() {
    return startTimestamp;
  }

  @Override
  public void setEndTimestamp(long endTimestamp) {
    this.endTimestamp = endTimestamp;
  }

  @Override
  public long getEndTimestamp() {
    return endTimestamp;
  }
}
