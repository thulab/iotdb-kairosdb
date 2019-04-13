package cn.edu.tsinghua.iotdb.kairosdb.query;

import cn.edu.tsinghua.iotdb.kairosdb.query.group_by.GroupBy;
import com.google.gson.annotations.SerializedName;
import java.util.List;
import java.util.Map;

public class QueryMetric {

  @SerializedName("name")
  private String name;

  @SerializedName("limit")
  private Long limit;

  @SerializedName("tags")
  private Map<String, List<String>> tags;

  @SerializedName("aggregators")
  private List<QueryAggregator> aggregators;

  @SerializedName("group_by")
  private List<GroupBy> groupBy;

}
