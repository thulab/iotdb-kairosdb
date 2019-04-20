package cn.edu.tsinghua.iotdb.kairosdb.query.aggregator;

public interface QueryAggregatorAlignable {

  void setAlign(QueryAggregatorAlign align);
  QueryAggregatorAlign getAlign();

  void setStartTimestamp(long startTimestamp);
  long getStartTimestamp();

  void setEndTimestamp(long endTimestamp);
  long getEndTimestamp();
  
}
