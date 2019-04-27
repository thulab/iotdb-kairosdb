package cn.edu.tsinghua.iotdb.kairosdb.query.aggregator;

import cn.edu.tsinghua.iotdb.kairosdb.datastore.Duration;
import cn.edu.tsinghua.iotdb.kairosdb.query.QueryException;
import cn.edu.tsinghua.iotdb.kairosdb.query.result.MetricResult;
import cn.edu.tsinghua.iotdb.kairosdb.query.result.MetricValueResult;
import cn.edu.tsinghua.iotdb.kairosdb.query.result.QueryDataPoint;
import java.util.List;

public class QueryAggregatorSum extends QueryAggregator
    implements QueryAggregatorSampling, QueryAggregatorAlignable {

  private Duration sampling;

  private QueryAggregatorAlign align;

  private long startTimestamp;
  private long endTimestamp;

  QueryAggregatorSum() {
    super(QueryAggregatorType.SUM);
  }

  @Override
  public MetricResult doAggregate(MetricResult result) throws QueryException {
    return useMethodAggregate(this, result);
  }

  @Override
  public MetricValueResult aggregate(MetricValueResult valueResult) throws QueryException {
    MetricValueResult newValueResult = new MetricValueResult(valueResult.getName());

    long step = getSampling().toTimestamp();

    List<List<QueryDataPoint>> splitPoints =
        valueResult.splitDataPoint(getStartTimestamp(), step, getAlign());

    for (List<QueryDataPoint> points : splitPoints) {

      long tmpTimestamp = 0L;
      boolean isTimestampGotten = false;

      int tmpInt = 0;
      int intCounter = 0;

      double tmpDouble = 0.0;
      int doubleCounter = 0;

      for (QueryDataPoint point : points) {
        if (!isTimestampGotten) {
          isTimestampGotten = true;
          tmpTimestamp = computeTimestampByAlign(this, point.getTimestamp(), step);
        }
        if (point.isInteger()) {
          tmpInt += point.getIntValue();
          intCounter++;
        } else {
          tmpDouble += point.getDoubleValue();
          doubleCounter++;
        }
      }
      if (intCounter > 0) {
        newValueResult.addDataPoint(new QueryDataPoint(tmpTimestamp, tmpInt));
      } else if (doubleCounter > 0) {
        newValueResult.addDataPoint(new QueryDataPoint(tmpTimestamp, tmpDouble));
      } else {
        throw new QueryException(
            "Among sum aggregator, there is an error in QueryAggregatorSum.aggregate");
      }

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
