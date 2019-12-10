package cn.edu.tsinghua.iotdb.kairosdb.query.result;

import com.google.gson.annotations.SerializedName;
import java.util.LinkedList;
import java.util.List;

public class QueryResult {

  @SerializedName("queries")
  private List<MetricResult> queries;

  public QueryResult() {
    queries = new LinkedList<>();
  }

  public List<MetricResult> getQueries() {
    return queries;
  }

  public void addMetricResult(MetricResult metricResult) {
    queries.add(metricResult);
  }

  public void addVoidMetricResult(String metricName) {
    MetricResult metricResult = new MetricResult();
    metricResult.addResult(new MetricValueResult(metricName));
    metricResult.getResults().get(0).setGroupBy(null);
    this.queries.add(metricResult);
  }

}
