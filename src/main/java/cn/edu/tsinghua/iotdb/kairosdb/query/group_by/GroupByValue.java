package cn.edu.tsinghua.iotdb.kairosdb.query.group_by;

public class GroupByValue extends GroupBy {

  private long rangeSize;

  public GroupByValue() {
    super(GroupByKind.VALUE);
  }

  public long getRangeSize() {
    return rangeSize;
  }

  public void setRangeSize(long rangeSize) {
    this.rangeSize = rangeSize;
  }

}
