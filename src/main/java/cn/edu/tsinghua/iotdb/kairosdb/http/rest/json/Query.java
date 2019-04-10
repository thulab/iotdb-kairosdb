package cn.edu.tsinghua.iotdb.kairosdb.http.rest.json;

import com.google.gson.annotations.SerializedName;
import javax.validation.constraints.Min;

public class Query {

  @SerializedName("start_absolute")
  private Long startAbsolute;

  @SerializedName("end_absolute")
  private Long endAbsolute;

  @SerializedName("start_relative")
  private Long startRelative;

  @SerializedName("end_relative")
  private Long endRelative;

  @Min(0)
  @SerializedName("cache_time")
  private int cache_time;


  public class TimeUnit {

    @SerializedName("value")
    private String value;

    @SerializedName("unit")
    private String unit;

  }

}

