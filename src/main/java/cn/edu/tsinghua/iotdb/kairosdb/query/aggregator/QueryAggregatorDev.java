package cn.edu.tsinghua.iotdb.kairosdb.query.aggregator;

import cn.edu.tsinghua.iotdb.kairosdb.datastore.Duration;
import cn.edu.tsinghua.iotdb.kairosdb.query.QueryException;
import cn.edu.tsinghua.iotdb.kairosdb.query.result.MetricResult;
import cn.edu.tsinghua.iotdb.kairosdb.query.result.MetricValueResult;
import cn.edu.tsinghua.iotdb.kairosdb.query.result.QueryDataPoint;
import java.util.List;


public class QueryAggregatorDev extends QueryAggregator
    implements QueryAggregatorSampling, QueryAggregatorAlignable {

  private Duration sampling;

  private QueryAggregatorAlign align;

  private long startTimestamp;
  private long endTimestamp;

  private static final int VALUE_TYPE = 0;
  private static final int POS_SD_TYPE = 1;
  private static final int NEG_SD_TYPE = 2;

  private int returnType;

  QueryAggregatorDev() {
    super(QueryAggregatorType.DEV);
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
      long tmpTimestamp = computeTimestampByAlign(this, points.get(0).getTimestamp(), step);

      double[] result = computeStandardDeviation(points);

      QueryDataPoint point;

      switch (getReturnType()) {
        case VALUE_TYPE:
          point = new QueryDataPoint(tmpTimestamp, result[0]);
          break;
        case POS_SD_TYPE:
          point = new QueryDataPoint(tmpTimestamp, result[1] + result[0]);
          break;
        case NEG_SD_TYPE:
          point = new QueryDataPoint(tmpTimestamp, result[1] - result[0]);
          break;
        default:
          throw new QueryException("Among dev aggregator, return_type must be specified.");
      }

      newValueResult.addDataPoint(point);

    }

    return newValueResult;
  }

  private double[] computeStandardDeviation(List<QueryDataPoint> points) {
    int size = points.size();
    double sum = 0;
    for (QueryDataPoint point : points) {
      sum += point.getAsDouble();
    }
    double avg = sum / size;
    sum = 0;
    for (QueryDataPoint point : points) {
      double value = point.getAsDouble();
      sum += (value - avg) * (value - avg);
    }
    return new double[]{Math.sqrt(sum), avg};
  }

  boolean setReturnTypeFromString(String returnType) {
    if (returnType == null) {
      return false;
    }
    switch (returnType) {
      case "value":
        this.returnType = QueryAggregatorDev.VALUE_TYPE;
        break;
      case "pos_sd":
        this.returnType = QueryAggregatorDev.POS_SD_TYPE;
        break;
      case "neg_sd":
        this.returnType = QueryAggregatorDev.NEG_SD_TYPE;
        break;
      default:
        return false;
    }
    return true;
  }

  private int getReturnType() {
    return returnType;
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
