package cn.edu.tsinghua.iotdb.kairosdb.query;

import cn.edu.tsinghua.iotdb.kairosdb.conf.Config;
import cn.edu.tsinghua.iotdb.kairosdb.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.kairosdb.dao.MetricsManager;
import cn.edu.tsinghua.iotdb.kairosdb.dao.SegmentManager;
import cn.edu.tsinghua.iotdb.kairosdb.http.rest.json.TimeUnitDeserializer;
import cn.edu.tsinghua.iotdb.kairosdb.query.aggregator.QueryAggregator;
import cn.edu.tsinghua.iotdb.kairosdb.query.aggregator.QueryAggregatorAlignable;
import cn.edu.tsinghua.iotdb.kairosdb.query.aggregator.QueryAggregatorDeserializer;
import cn.edu.tsinghua.iotdb.kairosdb.query.aggregator.QueryAggregatorType;
import cn.edu.tsinghua.iotdb.kairosdb.query.group_by.GroupBy;
import cn.edu.tsinghua.iotdb.kairosdb.query.group_by.GroupByDeserializer;
import cn.edu.tsinghua.iotdb.kairosdb.query.group_by.GroupBySerializer;
import cn.edu.tsinghua.iotdb.kairosdb.query.result.MetricResult;
import cn.edu.tsinghua.iotdb.kairosdb.query.result.MetricValueResult;
import cn.edu.tsinghua.iotdb.kairosdb.query.result.QueryDataPoint;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryWorker implements Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(QueryWorker.class);
  private static final Config config = ConfigDescriptor.getInstance().getConfig();
  private CountDownLatch queryLatch;
  private List<StringBuilder> queryMetricStrList;
  private QueryMetric metric;
  private Map<String, Integer> tag2pos;
  private Map<Integer, String> pos2tag;
  private Long startTime;
  private Long endTime;
  private List<MetricResult> metricResultList;
  private int metricCount;
  private long[] timeVertex;
  private List<List<List<Connection>>> connections;

  public QueryWorker(int metricCount, CountDownLatch queryLatch,
      List<StringBuilder> queryMetricStrList,
      QueryMetric metric, List<MetricResult> metricResultList, SegmentManager segmentManager) {
    this.metricResultList = metricResultList;
    this.queryLatch = queryLatch;
    this.queryMetricStrList = queryMetricStrList;
    this.metric = metric;
    this.startTime = segmentManager.getStartTime();
    this.endTime = segmentManager.getEndTime();
    this.metricCount = metricCount;
    timeVertex = segmentManager.getTimeVertex();
    connections = segmentManager.getConnections();
  }

  @Override
  public void run() {


    MetricResult metricResult = new MetricResult();
    try {
      if (getMetricMapping(metric)) {
        MetricValueResult metricValueResult = new MetricValueResult(metric.getName());
        long interval = endTime - startTime;
        metricResult.setSampleSize(getValueResult(metricValueResult));

        if (metricResult.getSampleSize() == 0) {
          metricResult = new MetricResult();
          metricResult.addResult(new MetricValueResult(metric.getName()));
          metricResult.getResults().get(0).setGroupBy(null);
        } else {
          metricResult.addResult(metricValueResult);
          if (!(metric.getAggregators().size() == 1 && metric.getAggregators().get(0).getType()
              .equals(
                  QueryAggregatorType.AVG) || interval > config.MAX_RANGE)) {
            metricResult = doAggregations(metric, metricResult);
          }
        }
      } else {
        metricResult = new MetricResult();
        metricResult.addResult(new MetricValueResult(metric.getName()));
        metricResult.getResults().get(0).setGroupBy(null);
      }

      if (metricResultList == null) {
        Gson gson = new GsonBuilder()
            .registerTypeAdapter(QueryMetric.class, new QueryMetric())
            .registerTypeAdapter(GroupBy.class, new GroupByDeserializer())
            .registerTypeAdapter(GroupBy.class, new GroupBySerializer())
            .registerTypeAdapter(QueryAggregator.class, new QueryAggregatorDeserializer())
            .registerTypeAdapter(
                cn.edu.tsinghua.iotdb.kairosdb.datastore.TimeUnit.class, new TimeUnitDeserializer())
            .registerTypeAdapter(QueryDataPoint.class, new QueryDataPoint())
            .create();
        queryMetricStrList.add(new StringBuilder(gson.toJson(metricResult)));
      } else {
        metricResultList.add(metricResult);
      }
    } catch (Exception e) {
      LOGGER.error("{} execute query failed because", Thread.currentThread().getName(), e);
    } finally {
      queryLatch.countDown();
      LOGGER.debug("{} Query Worker finished", Thread.currentThread().getName());
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

  private long getValueResult(MetricValueResult metricValueResult) {
    AtomicLong sampleSize = new AtomicLong(0);
    if (metricValueResult == null) {
      return 0L;
    }
    AtomicBoolean hasMetaData = new AtomicBoolean(false);
    int timeSegmentNum = timeVertex.length - 1;
    CountDownLatch segmentQueryLatch = new CountDownLatch(timeSegmentNum);
    for (int timeSegmentIndex = 0; timeSegmentIndex < timeSegmentNum; timeSegmentIndex++) {
      long segmentStartTime = timeVertex[timeSegmentIndex];
      long segmentEndTime = timeVertex[timeSegmentIndex + 1];
      QueryExecutor.getQueryWorkerPool().submit(new TimeSegmentQueryWorker(segmentStartTime,
          segmentEndTime, metric, metricValueResult, connections, hasMetaData,
          sampleSize, metricCount, segmentQueryLatch, timeSegmentIndex));
    }
    try {
      segmentQueryLatch.await();
      LOGGER.debug("All Segment Query Worker finished");
    } catch (InterruptedException e) {
      LOGGER.error("Exception occurred during waiting for all segment threads finish.", e);
      Thread.currentThread().interrupt();
    }
    return sampleSize.get();
  }

}
