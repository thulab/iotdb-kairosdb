package cn.edu.tsinghua.iotdb.kairosdb.query;

import cn.edu.tsinghua.iotdb.kairosdb.conf.Config;
import cn.edu.tsinghua.iotdb.kairosdb.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.kairosdb.dao.IoTDBConnectionPool;
import cn.edu.tsinghua.iotdb.kairosdb.dao.MetricsManager;
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
import java.util.ArrayList;
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
  private List<List<Connection>> connections = new ArrayList<>();

  public QueryWorker(int metricCount, CountDownLatch queryLatch,
      List<StringBuilder> queryMetricStrList,
      QueryMetric metric, List<MetricResult> metricResultList,
      Long startTime, Long endTime) {
    this.metricResultList = metricResultList;
    this.queryLatch = queryLatch;
    this.queryMetricStrList = queryMetricStrList;
    this.metric = metric;
    this.startTime = startTime;
    this.metricCount = metricCount;
    if (endTime - startTime > config.TIME_EDGE) {
      this.endTime = startTime + config.TIME_EDGE;
    } else {
      this.endTime = endTime;
    }
  }

  @Override
  public void run() {
    int splitSize = config.TIME_DIMENSION_SPLIT.size();

    long[] timeSplit = new long[splitSize + 1];
    for (int m = 0; m < splitSize; m++) {
      timeSplit[m] = config.TIME_DIMENSION_SPLIT.get(m);
    }
    timeSplit[splitSize] = System.currentTimeMillis();
    int startZone = 0;
    for (int i = 0; i < timeSplit.length; i++) {
      if (this.startTime <= timeSplit[i]) {
        startZone = i;
        break;
      } else {
        startZone = i + 1;
      }
    }
    int endZone = 0;
    for (int j = timeSplit.length - 1; j >= 0; j--) {
      if (this.endTime >= timeSplit[j]) {
        endZone = j;
        break;
      } else {
        endZone = -1;
      }
    }

    StringBuilder schemaStr = new StringBuilder();
    for (List<String> tags : metric.getTags().values()) {
      for (String s : tags) {
        schemaStr.append(s).append(".");
      }
    }
    timeVertex = new long[endZone - startZone + 1 + 2];
    if (startZone > endZone) {
      timeVertex[0] = this.startTime;
      timeVertex[1] = this.endTime;
      if (startZone == timeSplit.length) {
        // query latest data, use write&read IoTDB Instance
        List<List<Connection>> writeReadConnections =
            IoTDBConnectionPool.getInstance().getWriteReadConnections();
        int lastTimeSegmentIndex = writeReadConnections.size() - 1;
        List<Connection> latestWriteReadCons = writeReadConnections.get(lastTimeSegmentIndex);
        connections.add(latestWriteReadCons);
      } else {
        connections.add(IoTDBConnectionPool.getInstance().getReadConnections(startZone));
      }
    } else {
      int midIndex = 1;
      timeVertex[0] = this.startTime;
      timeVertex[timeVertex.length - 1] = this.endTime;
      for (int index = startZone; index <= endZone; index++) {
        timeVertex[midIndex] = timeSplit[index];
        midIndex++;
      }
      for (int zone = startZone; zone <= endZone + 1; zone++) {
        connections.add(IoTDBConnectionPool.getInstance().getReadConnections(zone));
      }
    }

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
    int segmentQueryWorkerNum = 0;
    for (List<Connection> connectionList : connections) {
      segmentQueryWorkerNum += connectionList.size();
    }
    CountDownLatch segmentQueryLatch = new CountDownLatch(segmentQueryWorkerNum);
    for (int timeSegmentIndex = 0; timeSegmentIndex < connections.size(); timeSegmentIndex++) {
      for (Connection connection : connections.get(timeSegmentIndex)) {
        long segmentStartTime = timeVertex[timeSegmentIndex];
        long segmentEndTime = timeVertex[timeSegmentIndex + 1];
        QueryExecutor.getQueryWorkerPool().submit(new SegmentQueryWorker(segmentStartTime,
            segmentEndTime, metric, metricValueResult, connection, hasMetaData,
            sampleSize, metricCount, segmentQueryLatch));
      }
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
