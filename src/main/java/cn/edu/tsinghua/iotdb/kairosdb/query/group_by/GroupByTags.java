package cn.edu.tsinghua.iotdb.kairosdb.query.group_by;

import java.util.LinkedList;
import java.util.List;

public class GroupByTags extends GroupBy {

  private List<String> tags;

  public GroupByTags() {
    super(GroupByKind.TAGS);
    tags = new LinkedList<>();
  }

  public List<String> getTags() {
    return tags;
  }

  public void addTag(String tag) {
    tags.add(tag);
  }
}
