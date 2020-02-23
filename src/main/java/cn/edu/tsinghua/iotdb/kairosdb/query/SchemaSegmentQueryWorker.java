package cn.edu.tsinghua.iotdb.kairosdb.query;

import cn.edu.tsinghua.iotdb.kairosdb.conf.Config;
import cn.edu.tsinghua.iotdb.kairosdb.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.kairosdb.dao.MetricsManager;
import cn.edu.tsinghua.iotdb.kairosdb.dao.SegmentManager;
import cn.edu.tsinghua.iotdb.kairosdb.profile.Measurement;
import cn.edu.tsinghua.iotdb.kairosdb.profile.Measurement.Profile;
import cn.edu.tsinghua.iotdb.kairosdb.query.aggregator.QueryAggregatorAvg;
import cn.edu.tsinghua.iotdb.kairosdb.query.aggregator.QueryAggregatorType;
import cn.edu.tsinghua.iotdb.kairosdb.query.group_by.GroupByType;
import cn.edu.tsinghua.iotdb.kairosdb.query.result.MetricValueResult;
import cn.edu.tsinghua.iotdb.kairosdb.query.result.QueryDataPoint;
import cn.edu.tsinghua.iotdb.kairosdb.query.sql_builder.DeleteSqlBuilder;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemaSegmentQueryWorker implements Runnable {
  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaSegmentQueryWorker.class);
  private static final Config config = ConfigDescriptor.getInstance().getConfig();
  private QueryMetric metric;
  private long segmentStartTime;
  private long segmentEndTime;
  private String sql;
  private CountDownLatch segmentQueryLatch;
  private int timeSegmentIndex;
  private int metricCount;
  private AtomicLong sampleSize;
  private MetricValueResult metricValueResult;
  private AtomicBoolean hasMetaData;
  private Map<Integer, List<String>> tmpTags;
  private Map<String, Integer> tag2pos;
  private Map<Integer, String> pos2tag;
  private List<List<List<Connection>>> connections;
  private String schemaKey;

  public SchemaSegmentQueryWorker(QueryContext queryContext,
      String sql, CountDownLatch schemaSegmentQueryLatch, AtomicLong sampleSize,
      MetricValueResult metricValueResult,
      AtomicBoolean hasMetaData, String schemaKey) {
    this.metric = queryContext.getMetric();
    this.segmentStartTime = queryContext.getSegmentStartTime();
    this.segmentEndTime = queryContext.getSegmentEndTime();
    this.sql = sql;
    this.segmentQueryLatch = schemaSegmentQueryLatch;
    this.timeSegmentIndex = queryContext.getTimeSegmentIndex();
    this.metricCount = queryContext.getMetricCount();
    this.sampleSize = sampleSize;
    this.metricValueResult = metricValueResult;
    this.hasMetaData = hasMetaData;
    this.connections = queryContext.getConnections();
    this.schemaKey = schemaKey;
    if (!getMetricMapping(metric)) {
      LOGGER.error("Get metric mapping of {} failed!", metric);
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


  @Override
  public void run() {
    try {
      long interval = segmentEndTime - segmentStartTime;
      //push down aggregation query
      if (metric.getAggregators().size() == 1 && metric.getAggregators().get(0).getType().equals(
          QueryAggregatorType.AVG) || interval > config.MAX_RANGE) {
        long value = config.GROUP_BY_UNIT;
        try {
          QueryAggregatorAvg queryAggregatorAvg = (QueryAggregatorAvg) metric.getAggregators()
              .get(0);
          value = queryAggregatorAvg.getSampling().toMillisecond();
        } catch (Exception e) {
          LOGGER.warn("Can't convert queryAggregatorAvg", e);
        }
        sql = sql.replace(metric.getName(), config.AGG_FUNCTION + "(" + metric.getName() + ")");
        sql = sql.substring(0, sql.indexOf("where"));
        sql = sql + " group by ("
            + value
            + "ms, ["
            + segmentStartTime
            + ", "
            + segmentEndTime
            + "])";
      }

      // use schema's hash code to use the query cache in IoTDB
      int schemaSegmentIndex = SegmentManager.writeSchemaHashCode(schemaKey, timeSegmentIndex);
      int readInstanceIndex = SegmentManager.readSchemaHashCode(metric.getName(), timeSegmentIndex,
          schemaSegmentIndex);
      Connection connection = connections.get(timeSegmentIndex).get(schemaSegmentIndex)
          .get(readInstanceIndex);
      long start = 0;
      if (config.ENABLE_PROFILER) {
        start = System.nanoTime();
      }
      try (Statement statement = connection.createStatement()) {
        LOGGER.debug("{} Send query SQL: {}", Thread.currentThread().getName(), sql);
        boolean isFirstNext = true;
        statement.execute(sql);
        ResultSet rs = statement.getResultSet();
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();
        long maxCount = config.POINT_EDGE / metricCount;
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
            sampleSize.incrementAndGet();
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
          if (sampleSize.get() > maxCount) {
            break;
          }
        }
        if (config.ENABLE_PROFILER) {
          Measurement.getInstance().add(Profile.IOTDB_QUERY, System.nanoTime() - start);
        }
        if (!hasMetaData.getAndSet(true)) {
          getTagValueFromPaths(metaData, paths);
          addBasicGroupByToResult(metaData, metricValueResult);
          setTags(metricValueResult);
        }
      } catch (SQLException e) {
        //TODO: add retry query, update connection if encountered error, retry once more
        LOGGER.error("{} Execute failed SQL: {}", Thread.currentThread().getName(), sql, e);
      }
    } catch (Exception e) {
      LOGGER.error("{} execute segment query failed because", Thread.currentThread().getName(), e);
    } finally {
      segmentQueryLatch.countDown();
      LOGGER.debug("{} Segment Query Worker finished", Thread.currentThread().getName());
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

  private void addBasicGroupByToResult(
      ResultSetMetaData metaData, MetricValueResult metricValueResult) throws SQLException {
    int type = metaData.getColumnType(2);
    if (type == Types.VARCHAR) {
      metricValueResult.addGroupBy(GroupByType.getTextTypeInstance());
    } else {
      metricValueResult.addGroupBy(GroupByType.getNumberTypeInstance());
    }
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
}
