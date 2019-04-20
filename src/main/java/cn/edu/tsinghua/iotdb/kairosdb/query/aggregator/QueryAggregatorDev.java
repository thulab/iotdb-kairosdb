package cn.edu.tsinghua.iotdb.kairosdb.query.aggregator;

import cn.edu.tsinghua.iotdb.kairosdb.datastore.Duration;
import cn.edu.tsinghua.iotdb.kairosdb.query.result.MetricResult;

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

  protected QueryAggregatorDev() {
    super(QueryAggregatorType.DEV);
  }

  public boolean setReturnTypeFromString(String returnType) {
    if (returnType == null) {
      return false;
    }
    if (returnType.equals("value")) {
      this.returnType = QueryAggregatorDev.VALUE_TYPE;
    } else if (returnType.equals("pos_sd")) {
      this.returnType = QueryAggregatorDev.POS_SD_TYPE;
    } else if (returnType.equals("neg_sd")) {
      this.returnType = QueryAggregatorDev.NEG_SD_TYPE;
    } else {
      return false;
    }
    return true;
  }

  public int getReturnType() {
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

  @Override
  public MetricResult doAggregate(MetricResult result) {
    return result;
  }

}
