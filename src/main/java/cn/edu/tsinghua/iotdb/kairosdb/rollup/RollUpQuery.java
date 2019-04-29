package cn.edu.tsinghua.iotdb.kairosdb.rollup;

import cn.edu.tsinghua.iotdb.kairosdb.query.Query;
import com.google.gson.annotations.SerializedName;

public class RollUpQuery {

  @SerializedName("save_as")
  private String saveAs;

  @SerializedName("query")
  private Query query;

  public String getSaveAs() {
    return saveAs;
  }

  public Query getQuery() {
    return query;
  }

}
