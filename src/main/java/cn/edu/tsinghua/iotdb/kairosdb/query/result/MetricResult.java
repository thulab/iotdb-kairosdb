package cn.edu.tsinghua.iotdb.kairosdb.query.result;

import com.google.gson.annotations.SerializedName;
import java.util.LinkedList;
import java.util.List;

public class MetricResult {

  @SerializedName("sample_size")
  private Long sampleSize;

  @SerializedName("results")
  private List<MetricValueResult> results;

  public MetricResult() {
    sampleSize = 0L;
    results = new LinkedList<>();
  }

  public Long getSampleSize() {
    return sampleSize;
  }

  public void setSampleSize(Long sampleSize) {
    this.sampleSize = sampleSize;
  }

  public List<MetricValueResult> getResults() {
    return results;
  }

  public void addResult(MetricValueResult result) {
    this.results.add(result);
  }

  public void setResults(List<MetricValueResult> results) {
    this.results = results;
  }

}
