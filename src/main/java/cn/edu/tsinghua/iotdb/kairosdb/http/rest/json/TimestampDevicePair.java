package cn.edu.tsinghua.iotdb.kairosdb.http.rest.json;

public class TimestampDevicePair {

  private long timestamp;
  private String device;
  private String key;

  public TimestampDevicePair(long timestamp, String device) {
    this.timestamp = timestamp;
    this.device = device;
    this.key = timestamp + device;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public String getDevice() {
    return device;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TimestampDevicePair)) {
      return false;
    }
    TimestampDevicePair pn = (TimestampDevicePair) o;
    return pn.timestamp == timestamp && pn.device.equals(device);
  }

  @Override
  public int hashCode() {
    return key.hashCode();
  }

}
