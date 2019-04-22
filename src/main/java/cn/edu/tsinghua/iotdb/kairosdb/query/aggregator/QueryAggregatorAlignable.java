package cn.edu.tsinghua.iotdb.kairosdb.query.aggregator;

import cn.edu.tsinghua.iotdb.kairosdb.query.QueryException;
import cn.edu.tsinghua.iotdb.kairosdb.query.result.MetricValueResult;

public interface QueryAggregatorAlignable {

  void setAlign(QueryAggregatorAlign align);
  QueryAggregatorAlign getAlign();

  MetricValueResult aggregate(MetricValueResult valueResult) throws QueryException;

  void setStartTimestamp(long startTimestamp);
  long getStartTimestamp();

  void setEndTimestamp(long endTimestamp);
  long getEndTimestamp();
  
}
