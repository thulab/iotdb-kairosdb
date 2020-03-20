package cn.edu.tsinghua.iotdb.kairosdb.query;

import cn.edu.tsinghua.iotdb.kairosdb.conf.Config;
import cn.edu.tsinghua.iotdb.kairosdb.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.kairosdb.conf.Constants;
import cn.edu.tsinghua.iotdb.kairosdb.dao.MetricsManager;
import cn.edu.tsinghua.iotdb.kairosdb.query.result.MetricValueResult;
import cn.edu.tsinghua.iotdb.kairosdb.query.sql_builder.QuerySqlBuilder;
import cn.edu.tsinghua.iotdb.kairosdb.tsdb.DBWrapper;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimeSegmentQueryWorker implements Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(TimeSegmentQueryWorker.class);
  private static final Config config = ConfigDescriptor.getInstance().getConfig();
  private long segmentEndTime;
  private long segmentStartTime;
  private QueryMetric metric;
  private Map<String, Integer> tag2pos;
  private Map<Integer, String> pos2tag;
  private Map<Integer, List<String>> tmpTags;
  private MetricValueResult metricValueResult;
  private List<List<List<DBWrapper>>> connections;
  private AtomicBoolean hasMetaData;
  private AtomicLong sampleSize;
  private int metricCount;
  private CountDownLatch segmentQueryLatch;
  private int timeSegmentIndex;

  public TimeSegmentQueryWorker(long segmentStartTime, long segmentEndTime, QueryMetric metric,
      MetricValueResult metricValueResult, List<List<List<DBWrapper>>> connections,
      AtomicBoolean hasMetaData,
      AtomicLong sampleSize, int metricCount, CountDownLatch segmentQueryLatch, int timeSegmentIndex) {
    this.segmentEndTime = segmentEndTime;
    this.segmentStartTime = segmentStartTime;
    this.metric = metric;
    this.metricValueResult = metricValueResult;
    this.connections = connections;
    this.hasMetaData = hasMetaData;
    this.sampleSize = sampleSize;
    this.metricCount = metricCount;
    this.segmentQueryLatch = segmentQueryLatch;
    this.timeSegmentIndex = timeSegmentIndex;
    getMetricMapping(metric);
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

  /*
  originSql example: SELECT test_query2 FROM root.*.DC2.server2,root.*.DC1.server1 where
  time>=1400000000000 and time<=1400000018000
   */
  private Map<String, StringBuilder> getDevicesMap(String[] sqlSplit) {
    String devicesStr = sqlSplit[Constants.DEVICE_POSITION];
    String[] devices = devicesStr.split(",");
    Map<String, StringBuilder> map = new HashMap<>();
    for(String device: devices) {
      String schemaKey = device.split("\\.")[Constants.SCHEMA_SEGMENT_PATH_INDEX];
      if(map.containsKey(schemaKey)) {
        map.get(schemaKey).append(",").append(device);
      } else {
        StringBuilder sameKeyDevices = new StringBuilder(device);
        map.put(schemaKey, sameKeyDevices);
      }
    }
    return map;
  }

  @Override
  public void run() {
    try {
      String sql = buildSqlStatement(metric, pos2tag, tag2pos.size(), segmentStartTime,
          segmentEndTime);
      String[] sqlSplit =  sql.split("\\s+");
      Map<String, StringBuilder> devicesMap = getDevicesMap(sqlSplit);
      CountDownLatch schemaSegmentQueryLatch = new CountDownLatch(devicesMap.size());
      QueryContext queryContext = new QueryContext();
      queryContext.setMetric(metric);
      queryContext.setSegmentStartTime(segmentStartTime);
      queryContext.setSegmentEndTime(segmentEndTime);
      queryContext.setTimeSegmentIndex(timeSegmentIndex);
      queryContext.setMetricCount(metricCount);
      queryContext.setConnections(connections);

      for(Map.Entry<String, StringBuilder> entry: devicesMap.entrySet()) {
        String schemaKey = entry.getKey();
        String sameSchemaKeyDevices = entry.getValue().toString();
        sqlSplit[Constants.DEVICE_POSITION] = sameSchemaKeyDevices;
        StringBuilder querySql = new StringBuilder();
        for(String shatteredSql: sqlSplit) {
          querySql.append(shatteredSql).append(" ");
        }
        QueryExecutor.getQueryWorkerPool().submit(new SchemaSegmentQueryWorker(queryContext,
            querySql.toString(), schemaSegmentQueryLatch, sampleSize, metricValueResult,
            hasMetaData, schemaKey));
      }
      try {
        schemaSegmentQueryLatch.await();
        LOGGER.debug("All Schema Segment Query Worker finished");
      } catch (InterruptedException e) {
        LOGGER.error("Exception occurred during waiting for all schema segment threads finish of "
            + "time segment {}", timeSegmentIndex, e);
        Thread.currentThread().interrupt();
      }

    } catch (Exception e) {
      LOGGER.error("{} execute segment query failed because", Thread.currentThread().getName(), e);
    } finally {
      segmentQueryLatch.countDown();
      LOGGER.debug("{} Segment Query Worker finished", Thread.currentThread().getName());
    }
  }
}
