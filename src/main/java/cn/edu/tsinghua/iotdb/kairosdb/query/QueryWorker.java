package cn.edu.tsinghua.iotdb.kairosdb.query;

import cn.edu.tsinghua.iotdb.kairosdb.conf.Config;
import cn.edu.tsinghua.iotdb.kairosdb.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.kairosdb.dao.IoTDBConnectionPool;
import cn.edu.tsinghua.iotdb.kairosdb.dao.MetricsManager;
import cn.edu.tsinghua.iotdb.kairosdb.http.rest.json.TimeUnitDeserializer;
import cn.edu.tsinghua.iotdb.kairosdb.profile.Measurement;
import cn.edu.tsinghua.iotdb.kairosdb.profile.Measurement.Profile;
import cn.edu.tsinghua.iotdb.kairosdb.query.aggregator.QueryAggregator;
import cn.edu.tsinghua.iotdb.kairosdb.query.aggregator.QueryAggregatorAlignable;
import cn.edu.tsinghua.iotdb.kairosdb.query.aggregator.QueryAggregatorDeserializer;
import cn.edu.tsinghua.iotdb.kairosdb.query.aggregator.QueryAggregatorType;
import cn.edu.tsinghua.iotdb.kairosdb.query.group_by.GroupBy;
import cn.edu.tsinghua.iotdb.kairosdb.query.group_by.GroupByDeserializer;
import cn.edu.tsinghua.iotdb.kairosdb.query.group_by.GroupBySerializer;
import cn.edu.tsinghua.iotdb.kairosdb.query.group_by.GroupByType;
import cn.edu.tsinghua.iotdb.kairosdb.query.result.MetricResult;
import cn.edu.tsinghua.iotdb.kairosdb.query.result.MetricValueResult;
import cn.edu.tsinghua.iotdb.kairosdb.query.result.QueryDataPoint;
import cn.edu.tsinghua.iotdb.kairosdb.query.sql_builder.DeleteSqlBuilder;
import cn.edu.tsinghua.iotdb.kairosdb.query.sql_builder.QuerySqlBuilder;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
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
import java.util.concurrent.CountDownLatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryWorker extends Thread {

  private static final Logger LOGGER = LoggerFactory.getLogger(QueryWorker.class);
  private static final Gson gson = new GsonBuilder()
      .registerTypeAdapter(QueryMetric.class, new QueryMetric())
      .registerTypeAdapter(GroupBy.class, new GroupByDeserializer())
      .registerTypeAdapter(GroupBy.class, new GroupBySerializer())
      .registerTypeAdapter(QueryAggregator.class, new QueryAggregatorDeserializer())
      .registerTypeAdapter(
          cn.edu.tsinghua.iotdb.kairosdb.datastore.TimeUnit.class, new TimeUnitDeserializer())
      .registerTypeAdapter(QueryDataPoint.class, new QueryDataPoint())
      .create();
  private static final Config config = ConfigDescriptor.getInstance().getConfig();
  private CountDownLatch queryLatch;
  private Map<String, StringBuilder> queryMetricStr;
  private QueryMetric metric;
  private Map<String, Integer> tag2pos;
  private Map<Integer, String> pos2tag;
  private Map<Integer, List<String>> tmpTags;
  private Long startTime;
  private Long endTime;


  public QueryWorker(CountDownLatch queryLatch, Map<String, StringBuilder> queryMetricStr,
      QueryMetric metric,
      Long startTime, Long endTime) {
    this.queryLatch = queryLatch;
    this.queryMetricStr = queryMetricStr;
    this.metric = metric;
    this.startTime = startTime;
    this.endTime = endTime;
  }

  @Override
  public void run() {
    try {
      MetricResult metricResult = new MetricResult();
      if (getMetricMapping(metric)) {
        MetricValueResult metricValueResult = new MetricValueResult(metric.getName());
        long interval = endTime - startTime;
        String sql = buildSqlStatement(metric, pos2tag, tag2pos.size(), startTime, endTime);
        if (metric.getAggregators().size() == 1 && metric.getAggregators().get(0).getType().equals(
            QueryAggregatorType.AVG) || interval > config.MAX_RANGE) {
          sql = sql.replace(metric.getName(), config.AGG_FUNCTION + "(" + metric.getName() + ")");
          sql = sql.substring(0, sql.indexOf("where"));
          String sqlBuilder = sql + " group by ("
              + config.GROUP_BY_UNIT
              + "ms, ["
              + startTime
              + ", "
              + endTime
              + "])";
          metricResult.setSampleSize(getValueResult(sqlBuilder, metricValueResult));
          setTags(metricValueResult);
          if (metricResult.getSampleSize() == 0) {
            metricResult = new MetricResult();
            metricResult.addResult(new MetricValueResult(metric.getName()));
            metricResult.getResults().get(0).setGroupBy(null);
          } else {
            metricResult.addResult(metricValueResult);
          }
        } else {
          metricResult.setSampleSize(getValueResult(sql, metricValueResult));
          setTags(metricValueResult);
          if (metricResult.getSampleSize() == 0) {
            metricResult = new MetricResult();
            metricResult.addResult(new MetricValueResult(metric.getName()));
            metricResult.getResults().get(0).setGroupBy(null);
          } else {
            metricResult.addResult(metricValueResult);
            metricResult = doAggregations(metric, metricResult);
          }
        }
      } else {
        metricResult = new MetricResult();
        metricResult.addResult(new MetricValueResult(metric.getName()));
        metricResult.getResults().get(0).setGroupBy(null);
      }
      queryMetricStr.put(metric.getName(), new StringBuilder(gson.toJson(metricResult)));
    } catch (Exception e) {
      LOGGER.error("{} execute query failed because", Thread.currentThread().getName(), e);
    } finally {
      queryLatch.countDown();
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

  private void setTags(MetricValueResult metricValueResult) {
    if (tmpTags != null) {
      for (Map.Entry<String, Integer> entry : tag2pos.entrySet()) {
        pos2tag.put(entry.getValue(), entry.getKey());
      }

      for (Map.Entry<Integer, List<String>> entry : tmpTags.entrySet()) {
        metricValueResult.setTag(pos2tag.get(entry.getKey() - 2), entry.getValue());
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

  private long getValueResult(String sql, MetricValueResult metricValueResult) {
    long sampleSize = 0L;
    if (sql == null || metricValueResult == null) {
      return sampleSize;
    }

    long start = 0;
    if (config.ENABLE_PROFILER) {
      start = System.nanoTime();
    }

    Connection connection = IoTDBConnectionPool.getInstance().getConnections().get(0);

    try (Statement statement = connection.createStatement()) {
      LOGGER.info("Send query SQL: {}", sql);
      boolean isFirstNext = true;
      statement.execute(sql);
      ResultSet rs = statement.getResultSet();
      ResultSetMetaData metaData = rs.getMetaData();
      int columnCount = metaData.getColumnCount();
      boolean[] paths = new boolean[columnCount - 1];
      while (rs.next()) {
        if (config.ENABLE_PROFILER && isFirstNext) {
          Measurement.getInstance().add(Profile.FIRST_NEXT, System.nanoTime() - start);
          isFirstNext = false;
        }
        long timestamp = rs.getLong(1);
        for (int i = 2; i <= columnCount; i++) {
          String value = rs.getString(i);
          if (value == null || value.equals(DeleteSqlBuilder.NULL_STR) || value
              .equals("2.147483646E9")) {
            continue;
          }
          sampleSize++;
          paths[i - 2] = true;
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
        }
      }
      if (config.ENABLE_PROFILER) {
        Measurement.getInstance().add(Profile.IOTDB_QUERY, System.nanoTime() - start);
      }
      getTagValueFromPaths(metaData, paths);

      addBasicGroupByToResult(metaData, metricValueResult);
    } catch (SQLException e) {
      LOGGER.warn(String.format("QueryExecutor.%s: %s", e.getClass().getName(), e.getMessage()));
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

  private void addBasicGroupByToResult(
      ResultSetMetaData metaData, MetricValueResult metricValueResult) throws SQLException {
    int type = metaData.getColumnType(2);
    if (type == Types.VARCHAR) {
      metricValueResult.addGroupBy(GroupByType.getTextTypeInstance());
    } else {
      metricValueResult.addGroupBy(GroupByType.getNumberTypeInstance());
    }
  }

}
