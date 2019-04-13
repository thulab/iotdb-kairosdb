package cn.edu.tsinghua.iotdb.kairosdb.query;

import cn.edu.tsinghua.iotdb.kairosdb.datastore.Duration;
import com.google.gson.annotations.SerializedName;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.NotNull.List;

public class QueryAggregator {

  @NotNull
  @SerializedName("name")
  private String name;

  @SerializedName("sampling")
  private Duration sampling;



}
