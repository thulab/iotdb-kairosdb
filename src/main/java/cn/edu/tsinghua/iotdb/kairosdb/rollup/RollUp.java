package cn.edu.tsinghua.iotdb.kairosdb.rollup;

import cn.edu.tsinghua.iotdb.kairosdb.datastore.Duration;
import com.google.gson.annotations.SerializedName;
import java.util.List;

public class RollUp {

  @SerializedName("name")
  private String name;

  @SerializedName("execution_interval")
  private Duration interval;

  @SerializedName("rollups")
  private List<RollUpQuery> rollups;

  public String getName() {
    return name;
  }

  public Duration getInterval() {
    return interval;
  }

  public List<RollUpQuery> getRollups() {
    return rollups;
  }
}
