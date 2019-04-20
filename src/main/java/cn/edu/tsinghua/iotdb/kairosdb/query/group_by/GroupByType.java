package cn.edu.tsinghua.iotdb.kairosdb.query.group_by;

import com.google.gson.annotations.SerializedName;

public class GroupByType extends GroupBy {

  @SerializedName("type")
  private String type;

  public GroupByType() {
    super(GroupByKind.TYPE);
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public static GroupByType getNumberTypeInstance() {
    GroupByType groupByType = new GroupByType();
    groupByType.setType("number");
    return groupByType;
  }

  public static GroupByType getTextTypeInstance() {
    GroupByType groupByType = new GroupByType();
    groupByType.setType("text");
    return groupByType;
  }
}
