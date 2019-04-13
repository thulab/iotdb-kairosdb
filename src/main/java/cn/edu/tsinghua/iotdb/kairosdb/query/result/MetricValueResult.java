package cn.edu.tsinghua.iotdb.kairosdb.query.result;

import com.google.gson.annotations.SerializedName;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class MetricValueResult {

  @SerializedName("name")
  private String name;

  @SerializedName("tags")
  private Map<String, List<String>> tags;

  @SerializedName("values")
  private List<QueryDataPoint> values;

  public MetricValueResult(String name) {
    this.name = name;
    tags = new HashMap<>();
    values = new LinkedList<>();
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Map<String, List<String>> getTags() {
    return tags;
  }

  public void setTags(Map<String, List<String>> tags) {
    this.tags = tags;
  }

  public void addTag(String key, String value) {
    List<String> list = tags.get(key);
    if (list == null) {
      list = new LinkedList<>();
    }
    list.add(value);
    tags.put(key, list);
  }

  public void addDataPoint(QueryDataPoint point) {
    values.add(point);
  }

}
