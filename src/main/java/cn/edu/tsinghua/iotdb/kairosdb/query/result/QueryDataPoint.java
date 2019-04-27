package cn.edu.tsinghua.iotdb.kairosdb.query.result;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import java.lang.reflect.Type;
import java.sql.Types;

public class QueryDataPoint implements JsonSerializer<QueryDataPoint>, Comparable<QueryDataPoint> {

  private Long timestamp;
  private int type;
  private Integer intValue;
  private Double doubleValue;
  private String text;

  public QueryDataPoint() {
  }

  public QueryDataPoint(Long timestamp) {
    this.timestamp = timestamp;
  }

  public QueryDataPoint(Long timestamp, int value) {
    this.timestamp = timestamp;
    this.intValue = value;
    this.type = Types.INTEGER;
  }

  public QueryDataPoint(Long timestamp, double value) {
    this.timestamp = timestamp;
    this.doubleValue = value;
    this.type = Types.DOUBLE;
  }

  public QueryDataPoint(Long timestamp, String value) {
    this.timestamp = timestamp;
    this.text = value;
    this.type = Types.VARCHAR;
  }

  public double getAsDouble() {
    switch (getType()) {
      case Types.INTEGER:
        return getIntValue();
      case Types.DOUBLE:
        return getDoubleValue();
      default:
        throw new IllegalArgumentException(
            "Among QueryDataPoint.getAsDouble(), type must be int or double");
    }
  }

  public String getAsString() {
    switch (getType()) {
      case Types.INTEGER:
        return String.valueOf(getIntValue());
      case Types.DOUBLE:
        return String.valueOf(getDoubleValue());
      case Types.VARCHAR:
        return getText();
      default:
        throw new IllegalArgumentException(
            "Among QueryDataPoint.getAsDouble(), type must be int or double");
    }
  }

  public void dividedBy(double value) {
    switch (getType()) {
      case Types.INTEGER:
        setIntValue(getIntValue() / (int) value);
        break;
      case Types.DOUBLE:
        setDoubleValue(getDoubleValue() / value);
        break;
      default:
        break;
    }
  }

  public Long getTimestamp() {
    return timestamp;
  }

  public Integer getIntValue() {
    return intValue;
  }

  private void setIntValue(int intValue) {
    this.intValue = intValue;
    this.type = Types.INTEGER;
  }

  public Double getDoubleValue() {
    return doubleValue;
  }

  private void setDoubleValue(double doubleValue) {
    this.doubleValue = doubleValue;
    this.type = Types.DOUBLE;
  }

  private String getText() {
    return text;
  }

  public int getType() {
    return type;
  }

  public boolean isInteger() {
    return this.type == Types.INTEGER;
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

  @Override
  public int compareTo(QueryDataPoint o) {
    if (type != o.getType()) {
      throw new IllegalArgumentException("When comparing QueryDataPoint, both types must be same.");
    }
    switch (type) {
      case Types.INTEGER:
        return getIntValue() - o.getIntValue();
      case Types.DOUBLE:
        double tDouble = getDoubleValue() - o.getDoubleValue();
        if (tDouble > 0) {
          return 1;
        } else if (tDouble < 0) {
          return -1;
        } else {
          return 0;
        }
      case Types.VARCHAR:
        return getText().compareTo(o.getText());
      default:
        return 0;
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof QueryDataPoint)) {
      throw new IllegalArgumentException(
          "In QueryDataPoint.equals(obj), obj must be of QueryDataPoint type.");
    }
    if (type != ((QueryDataPoint) obj).getType()) {
      throw new IllegalArgumentException("In QueryDataPoint.equals(), both types must be same.");
    }
    switch (type) {
      case Types.INTEGER:
        return getIntValue().equals(((QueryDataPoint) obj).getIntValue());
      case Types.DOUBLE:
        return getDoubleValue().equals(((QueryDataPoint) obj).getDoubleValue());
      case Types.VARCHAR:
        return getText().equals(((QueryDataPoint) obj).getText());
      default:
        return false;
    }
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }
}
