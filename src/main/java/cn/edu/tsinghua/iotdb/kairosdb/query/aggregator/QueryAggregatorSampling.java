package cn.edu.tsinghua.iotdb.kairosdb.query.aggregator;

import cn.edu.tsinghua.iotdb.kairosdb.datastore.Duration;

public interface QueryAggregatorSampling {

  void setSampling(Duration sampling);
  Duration getSampling();

}
