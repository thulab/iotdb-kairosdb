package cn.edu.tsinghua.iotdb.kairosdb.datastore.disruptor;

public class StringEvent {

  public String getValue() {
    return value;
  }

  public void set(String value) {
    this.value = value;
  }

  private String value;



}
