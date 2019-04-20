package cn.edu.tsinghua.iotdb.kairosdb.query.group_by;

import cn.edu.tsinghua.iotdb.kairosdb.datastore.Duration;
import cn.edu.tsinghua.iotdb.kairosdb.datastore.TimeUnit;

public class GroupByTime extends GroupBy {

  private int groupCount;

  private Duration rangeSize;

  public GroupByTime() {
    super(GroupByKind.TIME);
  }

  public int getGroupCount() {
    return groupCount;
  }

  public Duration getRangeSize() {
    return rangeSize;
  }

  public void setGroupCount(int groupCount) {
    this.groupCount = groupCount;
  }

  public void setGroupCount(String groupCount) {
    this.groupCount = Integer.parseInt(groupCount);
  }

  public void setRangeSize(Duration rangeSize) {
    this.rangeSize = rangeSize;
  }

  public void setRangeSize(int value, String unit) {
    this.rangeSize = new Duration(value, TimeUnit.from(unit));
  }
}
