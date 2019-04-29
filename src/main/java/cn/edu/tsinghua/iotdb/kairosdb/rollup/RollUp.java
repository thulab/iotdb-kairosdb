package cn.edu.tsinghua.iotdb.kairosdb.rollup;

import cn.edu.tsinghua.iotdb.kairosdb.datastore.Duration;
import com.google.gson.annotations.SerializedName;
import java.util.List;

public class RollUp implements Runnable{

  @SerializedName("name")
  private String name;

  @SerializedName("execution_interval")
  private Duration interval;

  @SerializedName("rollups")
  private List<RollUpQuery> rollups;

  private String id;

  private String json;

  public String getJson() {
    return json;
  }

  public void setJson(String json) {
    this.json = json;
  }

  public String getName() {
    return name;
  }

  public Duration getInterval() {
    return interval;
  }

  public List<RollUpQuery> getRollups() {
    return rollups;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  @Override
  public void run() {


    System.out.print("id = " + id + ", Thread name: " + Thread.currentThread().getName() + Thread.currentThread().getId());
    System.out.println(" name = " + name + ", execution_interval: " + interval.getValue() + " " + interval.getUnit());
  }

}
