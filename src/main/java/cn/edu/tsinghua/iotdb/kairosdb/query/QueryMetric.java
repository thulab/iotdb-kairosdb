package cn.edu.tsinghua.iotdb.kairosdb.query;

import cn.edu.tsinghua.iotdb.kairosdb.query.aggregator.QueryAggregator;
import cn.edu.tsinghua.iotdb.kairosdb.query.group_by.GroupBy;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.annotations.SerializedName;
import com.google.gson.reflect.TypeToken;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QueryMetric implements JsonDeserializer<QueryMetric> {

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

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Long getLimit() {
    return limit;
  }

  public void setLimit(Long limit) {
    this.limit = limit;
  }

  public Map<String, List<String>> getTags() {
    return tags;
  }

  public void setTags(Map<String, List<String>> tags) {
    this.tags = tags;
  }

  public List<QueryAggregator> getAggregators() {
    return aggregators;
  }

  public void setAggregators(
      List<QueryAggregator> aggregators) {
    this.aggregators = aggregators;
  }

  public List<GroupBy> getGroupBy() {
    return groupBy;
  }

  public void setGroupBy(List<GroupBy> groupBy) {
    this.groupBy = groupBy;
  }

  @Override
  public QueryMetric deserialize(JsonElement jsonElement, Type type,
      JsonDeserializationContext context) throws JsonParseException {
    JsonObject obj = jsonElement.getAsJsonObject();
    QueryMetric metric = new QueryMetric();
    metric.setName(context.deserialize(obj.get("name"), String.class));
    metric.setLimit(context.deserialize(obj.get("limit"), Long.class));
    JsonElement tagsEle = obj.get("tags");
    if (tagsEle == null) {
      metric.setTags(new HashMap<>());
    } else {
      Type tagsType = new TypeToken<Map<String, List<String>>>() {}.getType();
      metric.setTags(context.deserialize(tagsEle, tagsType));
    }
    JsonElement aggregatorEle = obj.get("aggregators");
    if (aggregatorEle == null) {
      metric.setAggregators(new ArrayList<>());
    } else {
      Type aggregatorType = new TypeToken<List<QueryAggregator>>() {}.getType();
      metric.setAggregators(context.deserialize(aggregatorEle, aggregatorType));
    }
    JsonElement groupByEle = obj.get("group_by");
    if (groupByEle == null) {
      metric.setGroupBy(new ArrayList<>());
    } else {
      Type groupByType = new TypeToken<List<GroupBy>>() {}.getType();
      metric.setGroupBy(context.deserialize(groupByEle, groupByType));
    }
    return metric;
  }
}
