package cn.edu.tsinghua.iotdb.kairosdb.rollup;

import cn.edu.tsinghua.iotdb.kairosdb.dao.MetricsManager;
import cn.edu.tsinghua.iotdb.kairosdb.datastore.Duration;
import cn.edu.tsinghua.iotdb.kairosdb.query.Query;
import cn.edu.tsinghua.iotdb.kairosdb.query.QueryException;
import cn.edu.tsinghua.iotdb.kairosdb.query.QueryExecutor;
import cn.edu.tsinghua.iotdb.kairosdb.query.result.MetricResult;
import cn.edu.tsinghua.iotdb.kairosdb.query.result.QueryResult;
import com.google.gson.annotations.SerializedName;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RollUp implements Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(RollUp.class);

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
    long currTime = System.currentTimeMillis();
    for (RollUpQuery rollUpQuery : rollups) {
      Query query = rollUpQuery.getQuery();
      query.setStartAbsolute(currTime - interval.toTimestamp());
      query.setEndAbsolute(currTime);
      QueryExecutor executor = new QueryExecutor(query);
      try {
        QueryResult queryResult = executor.execute();
        for (MetricResult metricResult : queryResult.getQueries()) {
          MetricsManager.addDataPoints(metricResult, rollUpQuery.getSaveAs());
        }
      } catch (QueryException e) {
        LOGGER.error("Execute Roll-up query failed because ", e);
      }
    }

    LOGGER.info("Roll-up id: {}, name: {}, execution_interval: {} {}",
        id, name, interval.getValue(), interval.getUnit());
  }

}
