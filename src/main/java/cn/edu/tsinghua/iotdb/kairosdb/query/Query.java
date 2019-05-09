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

  private Long getStartAbsolute() {
    return startAbsolute;
  }

  private Long getEndAbsolute() {
    return endAbsolute;
  }

  private Duration getStartRelative() {
    return startRelative;
  }

  private Duration getEndRelative() {
    return endRelative;
  }

  Long getStartTimestamp() {
    Long startTimestamp = getStartAbsolute();
    if (startTimestamp == null) {
      startTimestamp = getStartRelative().toRelatedTimestamp();
    }
    return startTimestamp;
  }

  Long getEndTimestamp() {
    Long endTimestamp = getEndAbsolute();
    if (endTimestamp == null && getEndRelative() != null) {
      endTimestamp = getEndRelative().toRelatedTimestamp();
    } else {
      endTimestamp = new Date().getTime();
    }
    return endTimestamp;
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

