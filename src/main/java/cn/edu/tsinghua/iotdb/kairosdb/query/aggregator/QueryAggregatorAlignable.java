package cn.edu.tsinghua.iotdb.kairosdb.query.aggregator;

public interface QueryAggregatorAlignable {

  void setAlign(QueryAggregatorAlign align);
  QueryAggregatorAlign getAlign();
  
}
