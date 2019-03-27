package cn.edu.tsinghua.iotdb.kairosdb.aggregator;


import cn.edu.tsinghua.iotdb.kairosdb.datastore.Duration;
import cn.edu.tsinghua.iotdb.kairosdb.datastore.TimeUnit;


public class Sampling extends Duration {

  public Sampling() {
    super();
  }

  public Sampling(int value, TimeUnit unit) {
    super(value, unit);
  }


  @Override
  public String toString() {
    return "Sampling{" +
        "} " + super.toString();
  }
}
