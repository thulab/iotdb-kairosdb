package cn.edu.tsinghua.iotdb.kairosdb.query;

import cn.edu.tsinghua.iotdb.kairosdb.datastore.Duration;
import com.google.gson.annotations.SerializedName;
import java.util.List;
import javax.validation.Valid;
import javax.validation.constraints.Min;

public class Query {

  @SerializedName("start_absolute")
  private Long startAbsolute;

  @SerializedName("end_absolute")
  private Long endAbsolute;

  @SerializedName("start_relative")
  private Duration startRelative;

  @SerializedName("end_relative")
  private Duration endRelative;

  @Min(0)
  @SerializedName("cache_time")
  private Long cacheTime;

  @Valid
  @SerializedName("time_zone")
  private String timeZone;

  @SerializedName("metrics")
  private List<QueryMetric> queryMetrics;

  public Long getStartAbsolute() {
    return startAbsolute;
  }

  public Long getEndAbsolute() {
    return endAbsolute;
  }

  public Duration getStartRelative() {
    return startRelative;
  }

  public Duration getEndRelative() {
    return endRelative;
  }

  public Long getCacheTime() {
    return cacheTime;
  }

  public String getTimeZone() {
    return timeZone;
  }

  public List<QueryMetric> getQueryMetrics() {
    return queryMetrics;
  }
}

