package cn.edu.tsinghua.iotdb.kairosdb.query.group_by;

public abstract class GroupBy {

  private final GroupByType type;

  public GroupBy(GroupByType type) {
    this.type = type;
  }

  public GroupByType getType() {
    return type;
  }

}
