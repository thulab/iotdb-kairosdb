package cn.edu.tsinghua.iotdb.kairosdb.query;

import cn.edu.tsinghua.iotdb.kairosdb.dao.IoTDBUtil;
import cn.edu.tsinghua.iotdb.kairosdb.dao.MetricsManager;
import cn.edu.tsinghua.iotdb.kairosdb.query.result.MetricResult;
import cn.edu.tsinghua.iotdb.kairosdb.query.result.MetricValueResult;
import cn.edu.tsinghua.iotdb.kairosdb.query.result.QueryDataPoint;
import cn.edu.tsinghua.iotdb.kairosdb.query.result.QueryResult;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
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

  public QueryResult execute() {

    Long startTime = query.getStartAbsolute();
    if (startTime == null) {
      startTime = query.getStartRelative().toTimeStamp();
    }
    Long endTime = query.getEndAbsolute();
    if (endTime == null) {
      endTime = query.getEndRelative().toTimeStamp();
    }

    QueryResult queryResult = new QueryResult();

    for (QueryMetric metric : query.getQueryMetrics()) {

      MetricResult metricResult = new MetricResult();

      Map<String, Integer> tag2pos = MetricsManager.getTagOrder(metric.getName());
      Map<Integer, String> pos2tag = new HashMap<>();
      boolean isOver = false;

      if (tag2pos == null) {
        isOver = true;
        queryResult.addVoidMetricResult(metric.getName());
      } else {
        for (Map.Entry<String, List<String>> tag : metric.getTags().entrySet()) {
          String tmpKey = tag.getKey();
          Integer tempPosition = tag2pos.getOrDefault(tmpKey, null);
          if (tempPosition == null) {
            isOver = true;
            queryResult.addVoidMetricResult(metric.getName());
          }
          pos2tag.put(tempPosition, tmpKey);
        }
      }

      if (!isOver) {
        String sql = buildSqlStatement(metric, pos2tag, tag2pos.size(), startTime, endTime);

        MetricValueResult metricValueResult = new MetricValueResult(metric.getName());
        metricValueResult.setTags(metric.getTags());

        metricResult.setSampleSize(getValueResult(sql, metricValueResult));
        metricResult.addResult(metricValueResult);

      }

      queryResult.addMetricResult(metricResult);

    }

    return queryResult;
  }

  private String buildSqlStatement(QueryMetric metric, Map<Integer, String> pos2tag, int maxPath,
      long startTime, long endTime) {
    QuerySqlBuilder sqlBuilder = new QuerySqlBuilder(metric.getName());

    for (int i = 0; i < maxPath; i++) {
      String tmpKey = pos2tag.getOrDefault(i, null);
      if (tmpKey == null) {
        sqlBuilder.append("*");
      } else {
        sqlBuilder.append(metric.getTags().get(tmpKey));
      }
    }

    return sqlBuilder.generateSql(startTime, endTime);
  }

  private long getValueResult(String sql, MetricValueResult metricValueResult) {
    long sampleSize = 0L;
    if (sql == null || metricValueResult == null) {
      return sampleSize;
    }

    Connection connection = IoTDBUtil.getConnection();
    try (Statement statement = connection.createStatement()) {
      statement.execute(sql);
      ResultSet rs = statement.getResultSet();
      ResultSetMetaData metaData = rs.getMetaData();
      int columnCount = metaData.getColumnCount();
      while (rs.next()) {
        long timestamp = rs.getLong(1);
        for (int i = 2; i <= columnCount; i++) {
          String value = rs.getString(i);
          if (value == null) {
            continue;
          }
          sampleSize++;
          QueryDataPoint dataPoint = null;
          switch (findType(value)) {
            case Types.INTEGER:
              int intValue = rs.getInt(i);
              dataPoint = new QueryDataPoint(timestamp, intValue);
              break;
            case Types.DOUBLE:
              double doubleValue = rs.getDouble(i);
              dataPoint = new QueryDataPoint(timestamp, doubleValue);
              break;
            case Types.VARCHAR:
              dataPoint = new QueryDataPoint(timestamp, value);
              break;
            default:
              LOGGER.error("QueryExecutor.execute: invalid type");
          }
          metricValueResult.addDataPoint(dataPoint);
          metaData.getColumnType(i);
        }
      }
    } catch (SQLException e) {
      LOGGER.warn(String.format("QueryExecutor.%s: %s", e.getClass().getName(), e.getMessage()));
    }
    return sampleSize;
  }

  private int findType(String string) {
    if (isNumeric(string)) {
      return Types.VARCHAR;
    } else {
      if (string.contains(".")) {
        return Types.DOUBLE;
      } else {
        return Types.INTEGER;
      }
    }
  }

  private boolean isNumeric(String string) {
    for (int i = 0; i < string.length(); i++) {
      if (!Character.isDigit(string.charAt(i))) {
        return false;
      }
    }
    return true;
  }

}
