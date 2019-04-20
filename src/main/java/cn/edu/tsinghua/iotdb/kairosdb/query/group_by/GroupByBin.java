package cn.edu.tsinghua.iotdb.kairosdb.query.group_by;

import java.util.LinkedList;
import java.util.List;

public class GroupByBin extends GroupBy {

  private List<String> bins;

  public GroupByBin() {
    super(GroupByKind.BIN);
    bins = new LinkedList<>();
  }

  public List<String> getBins() {
    return bins;
  }

  public void addBin(String bin) {
    bins.add(bin);
  }

}
