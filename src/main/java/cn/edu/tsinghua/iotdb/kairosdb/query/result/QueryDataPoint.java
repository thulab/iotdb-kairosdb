package cn.edu.tsinghua.iotdb.kairosdb.query.result;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import java.lang.reflect.Type;

public class QueryDataPoint implements JsonSerializer<QueryDataPoint> {

  private Long timestamp;
  private Integer intValue;
  private Double doubleValue;
  private String text;

  public QueryDataPoint() {
  }

  public QueryDataPoint(Long timestamp, int value) {
    this.timestamp = timestamp;
    this.intValue = value;
  }

  public QueryDataPoint(Long timestamp, double value) {
    this.timestamp = timestamp;
    this.doubleValue = value;
  }

  public QueryDataPoint(Long timestamp, String value) {
    this.timestamp = timestamp;
    this.text = value;
  }

  public Long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(Long timestamp) {
    this.timestamp = timestamp;
  }

  public Integer getIntValue() {
    return intValue;
  }

  public void setIntValue(int intValue) {
    this.intValue = intValue;
  }

  public Double getDoubleValue() {
    return doubleValue;
  }

  public void setDoubleValue(double doubleValue) {
    this.doubleValue = doubleValue;
  }

  public String getText() {
    return text;
  }

  public void setText(String text) {
    this.text = text;
  }

  @Override
  public JsonElement serialize(QueryDataPoint dataPoint, Type type,
      JsonSerializationContext jsonSerializationContext) {
    JsonArray array = new JsonArray();
    array.add(dataPoint.getTimestamp());

    if (dataPoint.getIntValue() != null) {
      array.add(dataPoint.getIntValue());
    } else if (dataPoint.getDoubleValue() != null) {
      array.add(dataPoint.getDoubleValue());
    } else if (dataPoint.getText() != null) {
      array.add(dataPoint.getText());
    }

    return array;
  }

}
