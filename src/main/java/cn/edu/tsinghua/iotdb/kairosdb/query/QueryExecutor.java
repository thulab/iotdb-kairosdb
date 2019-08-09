package cn.edu.tsinghua.iotdb.kairosdb.query;

import cn.edu.tsinghua.iotdb.kairosdb.conf.Config;
import cn.edu.tsinghua.iotdb.kairosdb.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.kairosdb.dao.IoTDBConnectionPool;
import cn.edu.tsinghua.iotdb.kairosdb.dao.IoTDBConnectionPool.ConnectionIterator;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryExecutor {

  public static final Logger LOGGER = LoggerFactory.getLogger(QueryExecutor.class);
  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  private Query query;

  private Long startTime;
  private Long endTime;

  private Map<String, Integer> tag2pos;
//  private Map<Integer, String> pos2tag;

  private String[] sortedTagKeys; //将指定的key和未指定的key（用*代替）按位置排序

  private Map<Integer, List<String>> tmpTags;

  public QueryExecutor(Query query) {
    this.query = query;
    this.startTime = query.getStartTimestamp();
    this.endTime = query.getEndTimestamp();
  }

  public QueryResult execute() throws QueryException {

    long start = System.currentTimeMillis();
    QueryResult queryResult = new QueryResult();
    if(config.DEBUG == 2) {
      long elapse = System.currentTimeMillis() - start;
      LOGGER.info("2.1 [parse query] cost {} ms", elapse);
      start = System.currentTimeMillis();
    }

    Map<String, List<String>> deviceMetrics = new HashMap<>();

    String prefix = "root.*";




    for (QueryMetric metric : query.getQueryMetrics()) {
      long start1 = System.currentTimeMillis();
      if (MetricsManager.checkAllTagExisted(metric.getName(), metric.getTags().keySet())) {
        //如果metric和tag都存在
        long start2 = System.currentTimeMillis();
        MetricResult metricResult = new MetricResult();

        String sql = buildSqlStatement(metric, startTime, endTime);
        LOGGER.info("Execute SQL: {}." , sql);
        MetricValueResult metricValueResult = new MetricValueResult(metric.getName());
        if(config.DEBUG == 4) {
          long elapse = System.currentTimeMillis() - start2;
          LOGGER.info("2.2.1.1 [build SQL statement and new Result class] of metric={} cost {} ms", metric.getName(), elapse);
          start2 = System.currentTimeMillis();
        }
        metricResult.setSampleSize(getValueResult(sql, metricValueResult));
        if(config.DEBUG == 4) {
          long elapse = System.currentTimeMillis() - start2;
          LOGGER.info("2.2.1.2 [metricResult.setSampleSize(getValueResult(sql, metricValueResult))] of metric={} cost {} ms", metric.getName(), elapse);
          start2 = System.currentTimeMillis();
        }
        setTags(metric.getName(), metricValueResult);
        if(config.DEBUG == 4) {
          long elapse = System.currentTimeMillis() - start2;
          LOGGER.info("2.2.1.3 [setTags(metricValueResult)] of metric={} cost {} ms", metric.getName(), elapse);
          start2 = System.currentTimeMillis();
        }
        if (metricResult.getSampleSize() == 0) {
          queryResult.addVoidMetricResult(metric.getName());
        } else {
          metricResult.addResult(metricValueResult);

          metricResult = doAggregations(metric, metricResult);

          queryResult.addMetricResult(metricResult);
          if(config.DEBUG == 4) {
            long elapse = System.currentTimeMillis() - start2;
            LOGGER.info("2.2.1.4 [doAggregations] of metric={} cost {} ms", metric.getName(), elapse);
          }
        }

      } else {
        queryResult.addVoidMetricResult(metric.getName());
      }
      if(config.DEBUG == 3) {
        long elapse = System.currentTimeMillis() - start1;
        LOGGER.info("2.2.1 [for (QueryMetric metric : query.getQueryMetrics())] of metric={} cost {} ms", metric.getName(), elapse);
      }
    }
    if(config.DEBUG == 2) {
      long elapse = System.currentTimeMillis() - start;
      LOGGER.info("2.2 [for (QueryMetric metric : query.getQueryMetrics())] loop cost {} ms", elapse);
    }

    return queryResult;
  }

  public void delete() {
    for (QueryMetric metric : query.getQueryMetrics()) {

      if (MetricsManager.checkAllTagExisted(metric.getName(), metric.getTags().keySet())) {

        String querySql = buildSqlStatement(metric, startTime, endTime);

        ConnectionIterator iterator = IoTDBConnectionPool.getInstance().getConnectionIterator();
        while(iterator.hasNext()) {
          Connection connection = iterator.next();
          try {
            Statement statement = connection.createStatement();
            statement.execute(querySql);

            ResultSet rs = statement.getResultSet();

            List<String> sqlList = buildDeleteSql(rs);
            statement = connection.createStatement();
            for (String sql : sqlList) {
              statement.addBatch(sql);
            }
            statement.executeBatch();

          } catch (SQLException e) {
            LOGGER.error(String.format("%s: %s", e.getClass().getName(), e.getMessage()));
          } finally {
            iterator.putBack(connection);
          }

        }
      }

    }
  }

  private String buildSqlStatement(QueryMetric metric,
      long startTime, long endTime) {

    String[] tagKeys = MetricsManager.getPosTagList(metric.getName());

    List<String>[] tagValues = new List[tagKeys.length];
    for (int i =0 ; i < tagKeys.length; i++) {
      if (metric.getTags().containsKey(tagKeys[i])) {
        tagValues[i] = metric.getTags().get(tagKeys[i]);
      } else {
        tagValues[i] = Collections.singletonList("*");
      }
    }
    List<String> subffixPaths = MetricsManager.productPatch(tagValues);

    StringBuilder sqlBuilder = new StringBuilder("SELECT ");

    for (int i =0; i < subffixPaths.size() - 1; i ++ ) {
      sqlBuilder.append(MetricsManager.getStorageGroupName(subffixPaths.get(i))).append(subffixPaths.get(i)).append(".").append(metric.getName()).append(",");
    }
    if (subffixPaths.size() > 0) {
      sqlBuilder.append(MetricsManager.getStorageGroupName(subffixPaths.get(subffixPaths.size() - 1))).append(subffixPaths.get(subffixPaths.size() - 1 )).append(".").append(metric.getName());
    }

    sqlBuilder.append(String.format(" FROM ROOT where time>=%s and time<=%s", startTime, endTime));
    return sqlBuilder.toString();
  }

  private List<String> buildDeleteSql(ResultSet rs) throws SQLException {
    ResultSetMetaData metaData = rs.getMetaData();

    String[] paths = new String[metaData.getColumnCount() - 1];
    int[] types = new int[metaData.getColumnCount() - 1];

    for (int i = 2; i <= metaData.getColumnCount(); i++) {
      paths[i - 2] = metaData.getColumnName(i);
      types[i - 2] = metaData.getColumnType(i);
    }

    DeleteSqlBuilder builder;
    builder = new DeleteSqlBuilder();

    while (rs.next()) {
      String timestamp = rs.getString(1);
      for (int i = 2; i <= metaData.getColumnCount(); i++) {
        if (rs.getString(i) != null) {
          builder.appendDataPoint(paths[i - 2], timestamp);
        }
      }
    }

    return builder.build(paths, types);
  }

  private long getValueResult(String sql, MetricValueResult metricValueResult) {
    long start = System.currentTimeMillis();
    long sampleSize = 0L;
    if (sql == null || metricValueResult == null) {
      return sampleSize;
    }
    LOGGER.info("start to execute query: {}", sql);
    ConnectionIterator iterator = IoTDBConnectionPool.getInstance().getConnectionIterator();
    while(iterator.hasNext()) {
      Connection connection = iterator.next();
      try (Statement statement = connection.createStatement()) {
        LOGGER.info("Send query SQL: {}", sql);
        statement.execute(sql);
        try (ResultSet rs = statement.getResultSet()) {
          ResultSetMetaData metaData = rs.getMetaData();
          int columnCount = metaData.getColumnCount();
          int type = metaData.getColumnType(2);
          boolean[] paths = new boolean[columnCount - 1];

          long start1 = System.currentTimeMillis();
          long nextStart = start1;
          long total = 0;
          while (rs.next()) {
            total += System.currentTimeMillis() - nextStart;
            long timestamp = rs.getLong(1);
            for (int i = 2; i <= columnCount; i++) {
              String value = rs.getString(i);
              //????
              if (value == null || value.equals(DeleteSqlBuilder.NULL_STR) || value
                  .equals("2.147483646E9")) {
                continue;
              }
              sampleSize++;
              paths[i - 2] = true;
              QueryDataPoint dataPoint = null;
              switch (type) {
                case Types.BIGINT:
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
            }
            nextStart = System.currentTimeMillis();
          }

          if (config.DEBUG == 5) {
            long elapse = System.currentTimeMillis() - start1;
            LOGGER.info("2.2.1.2.1 while (rs.next()) loop cost {} ms, rs.next() cost {} ms", elapse,
                total);
          }

          getTagValueFromPaths(metaData, paths);

          addBasicGroupByToResult(type, metricValueResult);
        }
        return sampleSize;
      } catch (Exception e) {
        LOGGER.error(String.format("QueryExecutor.%s: %s", e.getClass().getName(), e.getMessage()), e);
      } finally {
        iterator.putBack(connection);
        if (config.DEBUG == 5) {
          long elapse = System.currentTimeMillis() - start;
          LOGGER.info("2.2.1.2 [getValueResult()] cost {} ms", elapse);
        }
      }
    }
    return sampleSize;
  }

  private void getTagValueFromPaths(ResultSetMetaData metaData, boolean[] hasPaths)
      throws SQLException {
    tmpTags = new HashMap<>();
    int columnCount = metaData.getColumnCount();
    for (int i = 2; i <= columnCount; i++) {
      if (!hasPaths[i - 2]) {
        continue;
      }
      String[] paths = metaData.getColumnName(i).split("\\.");
      int pathsLen = paths.length;
      //第一个是root，第二个是sg，最后一个是metric name 全都舍弃
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

  private void setTags(String metricName, MetricValueResult metricValueResult) {
    if (tmpTags != null) {
      String[] tagNames = MetricsManager.getPosTagList(metricName);
      for (Map.Entry<Integer, List<String>> entry : tmpTags.entrySet()) {
        metricValueResult.setTag(tagNames[entry.getKey() - 2], entry.getValue());
      }
    }
  }

  private void addBasicGroupByToResult(
      int type, MetricValueResult metricValueResult) throws SQLException {
    if (type == Types.VARCHAR) {
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
      return Types.INTEGER;
    } else {
      if (string.contains(".")) {
        return Types.DOUBLE;
      } else {
        return Types.VARCHAR;
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
