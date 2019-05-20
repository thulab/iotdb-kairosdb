package cn.edu.tsinghua.iotdb.kairosdb.datastore.disruptor;

import com.lmax.disruptor.EventFactory;

public class StringEventFactory implements EventFactory<StringEvent> {

  @Override
  public StringEvent newInstance() {
    return new StringEvent();
  }
}
