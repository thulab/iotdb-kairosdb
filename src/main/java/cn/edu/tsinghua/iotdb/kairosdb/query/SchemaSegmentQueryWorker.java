package cn.edu.tsinghua.iotdb.kairosdb.query;

import cn.edu.tsinghua.iotdb.kairosdb.conf.Config;
import cn.edu.tsinghua.iotdb.kairosdb.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.kairosdb.dao.SegmentManager;
import cn.edu.tsinghua.iotdb.kairosdb.query.aggregator.QueryAggregatorAvg;
import cn.edu.tsinghua.iotdb.kairosdb.query.aggregator.QueryAggregatorType;
import cn.edu.tsinghua.iotdb.kairosdb.query.result.MetricValueResult;
import cn.edu.tsinghua.iotdb.kairosdb.tsdb.DBWrapper;
import java.util.List;
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
  private List<List<List<DBWrapper>>> connections;
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
      DBWrapper dbWrapper = connections.get(timeSegmentIndex).get(schemaSegmentIndex)
          .get(readInstanceIndex);
      dbWrapper.rangeQuery(sql, metricCount, sampleSize, metricValueResult, hasMetaData, metric);
    } catch (Exception e) {
      LOGGER.error("{} execute segment query failed because", Thread.currentThread().getName(), e);
    } finally {
      segmentQueryLatch.countDown();
      LOGGER.debug("{} Segment Query Worker finished", Thread.currentThread().getName());
    }
  }

}
