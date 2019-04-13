package cn.edu.tsinghua.iotdb.kairosdb.query;

import cn.edu.tsinghua.iotdb.kairosdb.dao.IoTDBUtil;
import cn.edu.tsinghua.iotdb.kairosdb.dao.MetricsManager;
import cn.edu.tsinghua.iotdb.kairosdb.query.result.QueryResult;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryExecutor {

  static final Logger LOGGER = LoggerFactory.getLogger(QueryExecutor.class);

  private Query query;

  public QueryExecutor(Query query) {
    this.query = query;
  }

  public QueryResult execute() throws QueryException {

    Map<String, Map<String, Integer>> tagOrders = MetricsManager.getTagOrder();

    Long startTime = query.getStartAbsolute();
    if (startTime == null) {
      startTime = query.getStartRelative().toTimeStamp();
    }
    Long endTime = query.getEndAbsolute();
    if (endTime == null) {
      endTime = query.getEndRelative().toTimeStamp();
    }

    QueryResult queryResult = new QueryResult();

    int metricCounter = 0;
    for (QueryMetric metric : query.getQueryMetrics()) {

      Map<String, Integer> tag2pos = tagOrders.get(metric.getName());
      Map<Integer, String> pos2tag = new HashMap<>();
      boolean isOver = false;

      for (Map.Entry<String, List<String>> tag : metric.getTags().entrySet()) {
        String tmpKey = tag.getKey();
        Integer tempPosition = tag2pos.getOrDefault(tmpKey, null);
        if (tempPosition == null) {
          isOver = true;
          queryResult.addVoidMetricResult(metric.getName());
        }
        pos2tag.put(tempPosition, tmpKey);
      }

      if (!isOver) {

        int maxPath = tag2pos.size();

        QuerySqlBuilder sqlBuilder = new QuerySqlBuilder(metric.getName());

        for (int i = 0; i < maxPath; i++) {
          String tmpKey = pos2tag.getOrDefault(i, null);
          if (tmpKey == null) {
            sqlBuilder.append("*");
          } else {
            sqlBuilder.append(metric.getTags().get(tmpKey));
          }
        }

        String sql = sqlBuilder.generateSql();

        Connection connection = IoTDBUtil.getConnection();
        try (Statement statement = connection.createStatement()) {
          statement.execute(sql);
          ResultSet rs = statement.getResultSet();
          while (rs.next()) {
            int i = 0;
          }
        } catch (SQLException e) {
          LOGGER.warn(String.format("QueryExecutor.%s: %s", e.getClass().getName(), e.getMessage()));
        }

      }

      metricCounter++;
    }

    return null;
  }

}
