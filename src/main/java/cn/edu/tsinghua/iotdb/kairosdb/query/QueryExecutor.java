package cn.edu.tsinghua.iotdb.kairosdb.query;

import cn.edu.tsinghua.iotdb.kairosdb.conf.Config;
import cn.edu.tsinghua.iotdb.kairosdb.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.kairosdb.dao.ConnectionPool;
import cn.edu.tsinghua.iotdb.kairosdb.dao.MetricsManager;
import cn.edu.tsinghua.iotdb.kairosdb.dao.SegmentManager;
import cn.edu.tsinghua.iotdb.kairosdb.query.aggregator.QueryAggregator;
import cn.edu.tsinghua.iotdb.kairosdb.query.aggregator.QueryAggregatorAlignable;
import cn.edu.tsinghua.iotdb.kairosdb.query.aggregator.QueryAggregatorType;
import cn.edu.tsinghua.iotdb.kairosdb.query.result.MetricResult;
import cn.edu.tsinghua.iotdb.kairosdb.query.result.MetricValueResult;
import cn.edu.tsinghua.iotdb.kairosdb.query.result.QueryResult;
import cn.edu.tsinghua.iotdb.kairosdb.query.sql_builder.QuerySqlBuilder;
import cn.edu.tsinghua.iotdb.kairosdb.tsdb.DBWrapper;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryExecutor {

  public static final Logger LOGGER = LoggerFactory.getLogger(QueryExecutor.class);
  private static final Config config = ConfigDescriptor.getInstance().getConfig();
  private static final ExecutorService queryWorkerPool = new ThreadPoolExecutor(
      config.CORE_POOL_SIZE,
      config.MAX_POOL_SIZE,
      300L, TimeUnit.SECONDS,
      new LinkedBlockingQueue<>());

  private Query query;
  private Long startTime;
  private Long endTime;
  private Map<String, Integer> tag2pos;
  private Map<Integer, String> pos2tag;
  private Map<Integer, List<String>> tmpTags;
  private SegmentManager segmentManager;

  public QueryExecutor(Query query) {
    this.query = query;
    this.startTime = query.getStartTimestamp();
    this.endTime = query.getEndTimestamp();
    if (endTime - startTime > config.TIME_EDGE) {
      this.endTime = startTime + config.TIME_EDGE;
    }
    segmentManager = new SegmentManager(startTime, endTime);
  }

  public static ExecutorService getQueryWorkerPool() {
    return queryWorkerPool;
  }

  public String executeV2() {
    StringBuilder queryResultStr = new StringBuilder();
    int queryMetricNum = query.getQueryMetrics().size();
    CountDownLatch queryLatch = new CountDownLatch(queryMetricNum);
    List<StringBuilder> queryMetricJsonsList = Collections.synchronizedList(new ArrayList<>());
    for (QueryMetric metric : query.getQueryMetrics()) {
      queryWorkerPool
          .submit(new QueryWorker(queryMetricNum, queryLatch, queryMetricJsonsList, metric,
              segmentManager));
    }
    try {
      // wait for all clients finish test
      queryLatch.await();
      LOGGER.debug("All Query Worker finished");
    } catch (InterruptedException e) {
      LOGGER.error("Exception occurred during waiting for all threads finish.", e);
      Thread.currentThread().interrupt();
    }
    StringBuilder midMetricBuilder = new StringBuilder();
    for (StringBuilder metricBuilder : queryMetricJsonsList) {
      midMetricBuilder.append(",").append(metricBuilder);
    }
    midMetricBuilder.delete(0, 1);
    queryResultStr.append("{\"queries\":[");
    if (queryMetricNum > 0) {
      queryResultStr.append(midMetricBuilder);
    }
    queryResultStr.append("]}");
    LOGGER.info("Query result string length:{}", queryResultStr.length());
    return queryResultStr.toString();
  }

  public QueryResult execute() throws QueryException {
    QueryResult queryResult = new QueryResult();
    for (QueryMetric metric : query.getQueryMetrics()) {
      if (getMetricMapping(metric)) {
        MetricResult metricResult = new MetricResult();
        MetricValueResult metricValueResult = new MetricValueResult(metric.getName());
        long interval = endTime - startTime;
        String sql = buildSqlStatement(metric, pos2tag, tag2pos.size(), startTime, endTime);
        if (metric.getAggregators().size() == 1 && metric.getAggregators().get(0).getType()
            .equals(QueryAggregatorType.AVG) || interval > config.MAX_RANGE) {
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
            queryResult.addVoidMetricResult(metric.getName());
          } else {
            metricResult.addResult(metricValueResult);
            queryResult.addMetricResult(metricResult);
          }
        } else {
          metricResult.setSampleSize(getValueResult(sql, metricValueResult));
          setTags(metricValueResult);
          if (metricResult.getSampleSize() == 0) {
            queryResult.addVoidMetricResult(metric.getName());
          } else {
            metricResult.addResult(metricValueResult);
            metricResult = doAggregations(metric, metricResult);
            queryResult.addMetricResult(metricResult);
          }

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
        for (List<DBWrapper> connectionList : ConnectionPool.getInstance()
            .getWriteReadConnections()) {
          for (DBWrapper dbWrapper : connectionList) {
            dbWrapper.delete(querySql);
          }
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

    for (List<DBWrapper> connectionList : ConnectionPool.getInstance()
        .getWriteReadConnections()) {
      for (DBWrapper dbWrapper : connectionList) {
        sampleSize += dbWrapper.getValueResult(sql, metricValueResult);
      }
    }

    return sampleSize;
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

}
