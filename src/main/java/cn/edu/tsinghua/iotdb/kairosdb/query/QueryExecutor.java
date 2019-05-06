package cn.edu.tsinghua.iotdb.kairosdb.query;

import cn.edu.tsinghua.iotdb.kairosdb.dao.IoTDBUtil;
import cn.edu.tsinghua.iotdb.kairosdb.dao.MetricsManager;
import cn.edu.tsinghua.iotdb.kairosdb.query.aggregator.QueryAggregator;
import cn.edu.tsinghua.iotdb.kairosdb.query.aggregator.QueryAggregatorAlignable;
import cn.edu.tsinghua.iotdb.kairosdb.query.group_by.GroupByType;
import cn.edu.tsinghua.iotdb.kairosdb.query.result.MetricResult;
import cn.edu.tsinghua.iotdb.kairosdb.query.result.MetricValueResult;
import cn.edu.tsinghua.iotdb.kairosdb.query.result.QueryDataPoint;
import cn.edu.tsinghua.iotdb.kairosdb.query.result.QueryResult;
import cn.edu.tsinghua.iotdb.kairosdb.query.sql_builder.DeleteSqlBuilder;
import cn.edu.tsinghua.iotdb.kairosdb.query.sql_builder.QuerySqlBuilder;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryExecutor {

  public static final Logger LOGGER = LoggerFactory.getLogger(QueryExecutor.class);

  private Query query;

  private Long startTime;
  private Long endTime;

  private Map<String, Integer> tag2pos;
  private Map<Integer, String> pos2tag;

  private Map<Integer, List<String>> tmpTags;

  public QueryExecutor(Query query) {
    this.query = query;
    this.startTime = query.getStartTimestamp();
    this.endTime = query.getEndTimestamp();
  }

  public QueryResult execute() throws QueryException {

    QueryResult queryResult = new QueryResult();

    for (QueryMetric metric : query.getQueryMetrics()) {

      if (getMetricMapping(metric)) {

        MetricResult metricResult = new MetricResult();

        String sql = buildSqlStatement(metric, pos2tag, tag2pos.size(), startTime, endTime);

        MetricValueResult metricValueResult = new MetricValueResult(metric.getName());

        metricResult.setSampleSize(getValueResult(sql, metricValueResult));

        if (tmpTags != null) {
          for (Map.Entry<String, Integer> entry : tag2pos.entrySet()) {
            pos2tag.put(entry.getValue(), entry.getKey());
          }

          for (Map.Entry<Integer, List<String>> entry :tmpTags.entrySet()) {
            metricValueResult.setTag(pos2tag.get(entry.getKey()-2), entry.getValue());
          }
        }

        if (metricResult.getSampleSize() == 0) {
          queryResult.addVoidMetricResult(metric.getName());
        } else {
          metricResult.addResult(metricValueResult);

          metricResult = doAggregations(metric, metricResult);

          queryResult.addMetricResult(metricResult);
        }

      } else {
        queryResult.addVoidMetricResult(metric.getName());
      }

    }

    return queryResult;
  }

  public void delete() {
    for (QueryMetric metric : query.getQueryMetrics()) {

      if (getMetricMapping(metric)) {
        String querySql = buildSqlStatement(metric, pos2tag, tag2pos.size(), startTime, endTime);

        try (Connection conn = IoTDBUtil.getNewConnection()) {
          Statement statement = conn.createStatement();
          statement.execute(querySql);

          ResultSet rs = statement.getResultSet();
          ResultSetMetaData rsmd = rs.getMetaData();

          String[] paths = new String[rsmd.getColumnCount() - 1];
          int[] types = new int[rsmd.getColumnCount() - 1];

          for (int i = 2; i <= rsmd.getColumnCount(); i++) {
            paths[i - 2] = rsmd.getColumnName(i);
            types[i - 2] = rsmd.getColumnType(i);
          }

          DeleteSqlBuilder builder;
          builder = new DeleteSqlBuilder();

          while (rs.next()) {
            String timestamp = rs.getString(1);
            for (int i = 2; i <= rsmd.getColumnCount(); i++) {
              if (rs.getString(i) != null) {
                builder.appendDataPoint(paths[i - 2], timestamp);
              }
            }
          }

          List<String> sqlList = builder.build(paths, types);
          statement = conn.createStatement();
          for (String sql : sqlList) {
            statement.addBatch(sql);
          }
          statement.executeBatch();

        } catch (SQLException | ClassNotFoundException e) {
          LOGGER.error(String.format("%s: %s", e.getClass().getName(), e.getMessage()));
        }

      }

    }
  }

  private boolean getMetricMapping(QueryMetric metric) {
    tag2pos = MetricsManager.getTagOrder(metric.getName());
    pos2tag = new HashMap<>();

    if (tag2pos == null) {
      return false;
    } else {
      for (Map.Entry<String, List<String>> tag : metric.getTags().entrySet()) {
        String tmpKey = tag.getKey();
        Integer tempPosition = tag2pos.getOrDefault(tmpKey, null);
        if (tempPosition == null) {
          return false;
        }
        pos2tag.put(tempPosition, tmpKey);
      }
    }

    return true;
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
          if (value == null || value.equals(DeleteSqlBuilder.NULL_STR) || value.equals("2.147483646E9")) {
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

      getTagValueFromPaths(metaData);

      addBasicGroupByToResult(metaData, metricValueResult);
    } catch (SQLException e) {
      LOGGER.warn(String.format("QueryExecutor.%s: %s", e.getClass().getName(), e.getMessage()));
    }
    return sampleSize;
  }

  private void getTagValueFromPaths(ResultSetMetaData metaData) throws SQLException {
    tmpTags = new HashMap<>();
    int columnCount = metaData.getColumnCount();
    for (int i = 2; i <= columnCount; i++) {
      String[] paths = metaData.getColumnName(i).split("\\.");
      int pathsLen = paths.length;
      for (int j = 2; j < pathsLen - 1; j++) {
        List<String> list = tmpTags.getOrDefault(j, null);
        if (list == null) {
          list = new LinkedList<>();
          tmpTags.put(j, list);
        }
        if (!list.contains(paths[j])) {
          list.add(paths[j]);
        }
      }
    }
  }

  private void addBasicGroupByToResult(
      ResultSetMetaData metaData, MetricValueResult metricValueResult) throws SQLException {
    String type = metaData.getColumnTypeName(1);
    if (type.equals("TEXT")) {
      metricValueResult.addGroupBy(GroupByType.getTextTypeInstance());
    } else {
      metricValueResult.addGroupBy(GroupByType.getNumberTypeInstance());
    }
  }

  private MetricResult doAggregations(QueryMetric metric, MetricResult result)
      throws QueryException {

    for (QueryAggregator aggregator : metric.getAggregators()) {
      if (aggregator instanceof QueryAggregatorAlignable) {
        ((QueryAggregatorAlignable) aggregator).setStartTimestamp(startTime);
        ((QueryAggregatorAlignable) aggregator).setEndTimestamp(endTime);
      }
      result = aggregator.doAggregate(result);
    }

    return result;
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
