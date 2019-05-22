package cn.edu.tsinghua.iotdb.kairosdb.query;

import cn.edu.tsinghua.iotdb.kairosdb.datastore.Duration;
import com.google.gson.annotations.SerializedName;
import java.util.Date;
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

  Long getStartTimestamp() {
    if (startAbsolute == null) {
      if(startRelative == null){
        return 0L;
      }
      return startRelative.toRelatedTimestamp();
    }
    return startAbsolute;
  }

  Long getEndTimestamp() {
    if (endAbsolute == null) {
      if(endRelative == null) {
        return new Date().getTime();
      }
      return endRelative.toRelatedTimestamp();
    }
    return endAbsolute;
  }

  public void setStartAbsolute(Long startAbsolute) {
    this.startAbsolute = startAbsolute;
  }

  public void setEndAbsolute(Long endAbsolute) {
    this.endAbsolute = endAbsolute;
  }

  public Long getCacheTime() {
    return cacheTime;
  }

  public String getTimeZone() {
    return timeZone;
  }

  List<QueryMetric> getQueryMetrics() {
    return queryMetrics;
  }
}

