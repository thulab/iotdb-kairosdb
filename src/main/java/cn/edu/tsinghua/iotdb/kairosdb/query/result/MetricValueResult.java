package cn.edu.tsinghua.iotdb.kairosdb.query.result;

import cn.edu.tsinghua.iotdb.kairosdb.query.group_by.GroupBy;
import com.google.gson.annotations.SerializedName;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class MetricValueResult {

  @SerializedName("name")
  private String name;

  @SerializedName("group_by")
  private List<GroupBy> groupBy;

  @SerializedName("tags")
  private Map<String, List<String>> tags;

  @SerializedName("values")
  private List<QueryDataPoint> values;

  public MetricValueResult(String name) {
    this.name = name;
    groupBy = new LinkedList<>();
    tags = new HashMap<>();
    values = new LinkedList<>();
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setGroupBy(List<GroupBy> groupBy) {
    this.groupBy = groupBy;
  }

  public void addGroupBy(GroupBy groupBy) {
    if (this.groupBy == null) {
      this.groupBy = new LinkedList<>();
    }
    if (groupBy == null) {
      return;
    }
    this.groupBy.add(groupBy);
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

  public void setTag(String key, List<String> tags) {
    this.tags.put(key, tags);
  }

  public void addDataPoint(QueryDataPoint point) {
    if (point == null)
      return;
    values.add(point);
  }

}
