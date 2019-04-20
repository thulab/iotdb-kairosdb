package cn.edu.tsinghua.iotdb.kairosdb.query.group_by;

public abstract class GroupBy {

  private final GroupByKind kind;

  public GroupBy(GroupByKind kind) {
    this.kind = kind;
  }

  public GroupByKind getKind() {
    return kind;
  }

}
