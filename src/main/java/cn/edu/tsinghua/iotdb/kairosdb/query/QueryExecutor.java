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

    List<QueryMetric> emptyMetrics = new ArrayList<>();
    for (QueryMetric metric : query.getQueryMetrics()) {
      if (!MetricsManager.checkAllTagExisted(metric.getName(), metric.getTags().keySet())) {
        queryResult.addVoidMetricResult(metric.getName());
        emptyMetrics.add(metric);
      }
    }
    query.getQueryMetrics().removeAll(emptyMetrics);

    if (query.getQueryMetrics().isEmpty()) {
      return queryResult;
    }

    Map<String, QueryMetric> querMetricMap = new HashMap<>();

    for (QueryMetric metric : query.getQueryMetrics()) {
      querMetricMap.put(metric.getName(), metric);
    }

    String sql = buildSqlStatement(query.getQueryMetrics(), startTime, endTime);

    LOGGER.info("Execute SQL: {}." , sql);

    MetricValueResult[] results = getValueResults(sql, query.getQueryMetrics());


    for (MetricValueResult metricValueResult : results) {
      MetricResult metricResult = new MetricResult();

      metricValueResult.setTags(querMetricMap.get(metricValueResult.getName()).getTags());


      if (metricValueResult.getDatapoints().size() == 0) {
        queryResult.addVoidMetricResult(metricValueResult.getName());
      } else {
        metricResult.addResult(metricValueResult);
        metricResult.setSampleSize(Long.valueOf(metricValueResult.getDatapoints().size()));
        metricResult = doAggregations(querMetricMap.get(metricValueResult.getName()), metricResult);
        queryResult.addMetricResult(metricResult);
      }
    }


    return queryResult;
  }

  private MetricValueResult[] getValueResults(String sql, List<QueryMetric> queryMetrics) {

    MetricValueResult[] results = new MetricValueResult[queryMetrics.size()];

    String[] metricPos = null;
    Map<String, MetricValueResult> metricValueResultMap = new HashMap<>();
    for(QueryMetric queryMetric: queryMetrics){
      MetricValueResult m = new MetricValueResult(queryMetric.getName());
      metricValueResultMap.put(queryMetric.getName(), m);
    }
    boolean hasMetricPos = false;

    long start = System.currentTimeMillis();

    ConnectionIterator iterator = IoTDBConnectionPool.getInstance().getConnectionIterator();
    if(iterator.hasNext()) {
      Connection connection = iterator.next();
      try (Statement statement = connection.createStatement()) {
        LOGGER.info("Send query SQL: {}", sql);
        statement.execute(sql);
        try (ResultSet rs = statement.getResultSet()) {
          ResultSetMetaData metaData = rs.getMetaData();
          int columnCount = metaData.getColumnCount();

          boolean[] paths = new boolean[columnCount - 1];

          long start1 = System.currentTimeMillis();
          long nextStart = start1;
          long total = 0;
          while (rs.next()) {
            total += System.currentTimeMillis() - nextStart;
            long timestamp = rs.getLong(1);
            if(!hasMetricPos) {
              metricPos = new String[columnCount - 1];
              for (int columnIndex = 2; columnIndex <= columnCount; columnIndex++) {// start with 1, and the first (i.e., 1) is the timestamp
                int type = metaData.getColumnType(columnIndex);
                String[] split = metaData.getColumnName(columnIndex).split("\\.");
                String metric = split[split.length - 1];
                metricPos[columnIndex - 2] = metric;
                addQueryDataPoint(metricPos, metricValueResultMap.get(metricPos[columnIndex - 2]), rs, metaData, paths, timestamp, columnIndex, type);
                metricValueResultMap.get(metricPos[columnIndex - 2]).addBasicGroupByToResult(type);
              }

              hasMetricPos = true;
            } else {
              for (int i = 2; i <= columnCount; i++) {
                int type = metaData.getColumnType(i);
                addQueryDataPoint(metricPos, metricValueResultMap.get(metricPos[i - 2]), rs, metaData, paths, timestamp, i, type);
              }
            }
            nextStart = System.currentTimeMillis();
          }

          if (config.DEBUG == 5) {
            long elapse = System.currentTimeMillis() - start1;
            LOGGER.info("2.2.1.2.1 while (rs.next()) loop cost {} ms, rs.next() cost {} ms", elapse,
                total);
          }
        }

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
    int i = 0;

    for(MetricValueResult m: metricValueResultMap.values()){
      results[i] = m;
      i++;
    }
    return results;
  }

  private void addQueryDataPoint(String[] metricPos, MetricValueResult metricValueResult,
      ResultSet rs, ResultSetMetaData metaData, boolean[] paths, long timestamp, int columnIndex,
      int type) throws SQLException {
    String value = rs.getString(columnIndex);

    if (value == null || value.equals(DeleteSqlBuilder.NULL_STR) || value
        .equals("2.147483646E9")) {
      return ;
    }

    paths[columnIndex - 2] = true;
    QueryDataPoint dataPoint = null;
    switch (type) {
      case Types.BIGINT:
      case Types.INTEGER:
        int intValue = rs.getInt(columnIndex);
        dataPoint = new QueryDataPoint(timestamp, intValue);
        break;
      case Types.DOUBLE:
        double doubleValue = rs.getDouble(columnIndex);
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

  public void delete() {
    for (QueryMetric metric : query.getQueryMetrics()) {

      if (MetricsManager.checkAllTagExisted(metric.getName(), metric.getTags().keySet())) {

        String querySql = buildSqlStatement(Collections.singletonList(metric), startTime, endTime);

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

  private String buildSqlStatement(List<QueryMetric> metrics, long startTime, long endTime) {
    StringBuilder sqlBuilder = new StringBuilder("SELECT ");
    for (QueryMetric metric : metrics) {
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
      for (String subfixPath :  subffixPaths) {
        sqlBuilder.append(MetricsManager.getStorageGroupName(subfixPath)).append(subfixPath).append(".").append(metric.getName()).append(",");
      }
    }
    sqlBuilder.deleteCharAt(sqlBuilder.length() - 1);
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


//  private void getTagValueFromPaths(ResultSetMetaData metaData, boolean[] hasPaths)
//      throws SQLException {
//    tmpTags = new HashMap<>();
//    int columnCount = metaData.getColumnCount();
//    for (int i = 2; i <= columnCount; i++) {
//      if (!hasPaths[i - 2]) {
//        continue;
//      }
//      String[] paths = metaData.getColumnName(i).split("\\.");
//      int pathsLen = paths.length;
//      //第一个是root，第二个是sg，最后一个是metric name 全都舍弃
//      for (int j = 2; j < pathsLen - 1; j++) {
//        List<String> list = tmpTags.getOrDefault(j, null);
//        if (list == null) {
//          list = new LinkedList<>();
//          tmpTags.put(j, list);
//        }
//        if (!list.contains(paths[j])) {
//          list.add(paths[j]);
//        }
//      }
//    }
//  }
//
//  private void setTags(String metricName, MetricValueResult metricValueResult) {
//    if (tmpTags != null) {
//      String[] tagNames = MetricsManager.getPosTagList(metricName);
//      for (Map.Entry<Integer, List<String>> entry : tmpTags.entrySet()) {
//        metricValueResult.setTag(tagNames[entry.getKey() - 2], entry.getValue());
//      }
//    }
//  }



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
