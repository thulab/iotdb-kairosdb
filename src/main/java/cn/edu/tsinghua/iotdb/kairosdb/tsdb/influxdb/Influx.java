package cn.edu.tsinghua.iotdb.kairosdb.tsdb.influxdb;

import cn.edu.tsinghua.iotdb.kairosdb.conf.Config;
import cn.edu.tsinghua.iotdb.kairosdb.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.kairosdb.dao.MetricsManager;
import cn.edu.tsinghua.iotdb.kairosdb.http.rest.json.DataPointsParser.DataType;
import cn.edu.tsinghua.iotdb.kairosdb.profile.Measurement;
import cn.edu.tsinghua.iotdb.kairosdb.profile.Measurement.Profile;
import cn.edu.tsinghua.iotdb.kairosdb.query.QueryMetric;
import cn.edu.tsinghua.iotdb.kairosdb.query.group_by.GroupByType;
import cn.edu.tsinghua.iotdb.kairosdb.query.result.MetricValueResult;
import cn.edu.tsinghua.iotdb.kairosdb.query.result.QueryDataPoint;
import cn.edu.tsinghua.iotdb.kairosdb.query.sql_builder.DeleteSqlBuilder;
import cn.edu.tsinghua.iotdb.kairosdb.tsdb.IDatabase;
import cn.edu.tsinghua.iotdb.kairosdb.util.Util;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import okhttp3.OkHttpClient;
import okhttp3.OkHttpClient.Builder;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.influxdb.dto.QueryResult.Result;
import org.influxdb.dto.QueryResult.Series;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Influx implements IDatabase {

  private static final Logger LOGGER = LoggerFactory.getLogger(Influx.class);
  private static final Config config = ConfigDescriptor.getInstance().getConfig();
  private InfluxDB influxDbInstance;
  private static final String DB_NAME = "ikr_data";
  private static final String DEFAULT_RP = "autogen";
  private static final String INFLUX_PATH_SPLIT = "_";
  private static final String TIMESTAMP_PRECISION = "ms";
  private Map<Integer, List<String>> tmpTags;
  private Map<String, Integer> tag2pos;
  private Map<Integer, String> pos2tag;

  public Influx(String influxUrl) {
    try {
      OkHttpClient.Builder client = new Builder().connectTimeout(5, TimeUnit.MINUTES).
          readTimeout(5, TimeUnit.MINUTES).writeTimeout(5, TimeUnit.MINUTES).
          retryOnConnectionFailure(true);
      influxDbInstance = InfluxDBFactory.connect(influxUrl, client);
    } catch (Exception e) {
      LOGGER.error("Initialize InfluxDB failed because ", e);
    }
    assert influxDbInstance != null;
    if (!influxDbInstance.databaseExists(DB_NAME)) {
      try {
        influxDbInstance.createDatabase(DB_NAME);
      } catch (Exception e) {
        LOGGER.error("create InfluxDB database failed because ", e);
      }
    }
  }

  private Number parseNumber(String value) {
    if (value.contains(".")) {
      return Double.parseDouble(value);
    } else {
      return Long.parseLong(value);
    }
  }

  @Override
  public void insert(String deviceId, long timestamp, List<String> sensors,
      List<String> values) {
    BatchPoints batchPoints = BatchPoints.database(DB_NAME)
        .retentionPolicy(DEFAULT_RP)
        .consistency(org.influxdb.InfluxDB.ConsistencyLevel.ALL).build();

    InfluxDataModel model = new InfluxDataModel();

    String[] splitDeviceId = deviceId.split("\\.");
    StringBuilder noStorageGroupDeviceIdBuilder = new StringBuilder();
    for (int i = 2; i < splitDeviceId.length; i++) {
      noStorageGroupDeviceIdBuilder.append(splitDeviceId[i]).append(INFLUX_PATH_SPLIT);
    }
    noStorageGroupDeviceIdBuilder.deleteCharAt(noStorageGroupDeviceIdBuilder.length() - 1);

    // use deviceId (without root.storageGroup) as measurement
    model.setMeasurement(noStorageGroupDeviceIdBuilder.toString());

    model.setTimestamp(timestamp);
    model.setTimestampPrecision(TIMESTAMP_PRECISION);
    HashMap<String, Object> fields = new HashMap<>();
    for (int i = 0; i < sensors.size(); i++) {
      if (Util.isNumber(values.get(i))) {
        Number value = parseNumber(values.get(i));
        fields.put(sensors.get(i), value);
      } else {
        fields.put(sensors.get(i), values.get(i));
      }
    }
    model.setFields(fields);
    batchPoints.point(model.toInfluxPoint());
    influxDbInstance.write(batchPoints);
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
  public void rangeQuery(String iotdbSql, long metricCount, AtomicLong sampleSize,
      MetricValueResult metricValueResult, AtomicBoolean hasMetaData, QueryMetric metric) {
    if (!getMetricMapping(metric)) {
      LOGGER.error("Get metric mapping of {} failed!", metric);
    }
    long start = 0;
    if (config.ENABLE_PROFILER) {
      start = System.nanoTime();
    }

    String influxSql = getInfluxSql(iotdbSql);
    LOGGER.debug("{} Send query SQL: {}", Thread.currentThread().getName(), influxSql);

    long maxCount = config.POINT_EDGE / metricCount;

    try {

      QueryResult results = influxDbInstance.query(new Query(influxSql, DB_NAME),
          TimeUnit.MILLISECONDS);
      for (Result result : results.getResults()) {
        List<Series> series = result.getSeries();
        if (series == null) {
          continue;
        }
        if (result.getError() != null) {
          LOGGER.error("{} error occurred when query InfluxDB: {}",
              Thread.currentThread().getName(), result.getError());
        }

        /*
         * example:
         * series: [Series [name=group_0, tags=null, columns=[time, test_query2], values=[[1.4E12,
         * 12.3], [1.400000001E12, 13.2], [1.400000002E12, 23.1], [1.400000003E12, 24.0], [1.400000004E12, 24.1]]]]
         */
        for (Series serie : series) {

          List<List<Object>> values = serie.getValues();
          for (List<Object> list : values) {
            long timestamp = (long) Double.parseDouble(list.get(0).toString());
            String value = list.get(1).toString();
            if (value == null || value.equals(DeleteSqlBuilder.NULL_STR) || value
                .equals("2.147483646E9")) {
              continue;
            }
            sampleSize.incrementAndGet();
            QueryDataPoint dataPoint = null;
            switch (Util.findType(value)) {
              case Types.INTEGER:
                int intValue = Integer.parseInt(value);
                dataPoint = new QueryDataPoint(timestamp, intValue);
                break;
              case Types.DOUBLE:
                double doubleValue = Double.parseDouble(value);
                dataPoint = new QueryDataPoint(timestamp, doubleValue);
                break;
              case Types.VARCHAR:
                dataPoint = new QueryDataPoint(timestamp, value);
                break;
              default:
                LOGGER.error("QueryExecutor.execute: invalid type");
            }
            metricValueResult.addDataPoint(dataPoint);
            if (sampleSize.get() > maxCount) {
              break;
            }
          }

          tmpTags = new HashMap<>();

          String[] paths = serie.getName().split(INFLUX_PATH_SPLIT);
          int pathsLen = paths.length;
          for (int j = 0; j < pathsLen; j++) {
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
      if (config.ENABLE_PROFILER) {
        Measurement.getInstance().add(Profile.IOTDB_QUERY, System.nanoTime() - start);
      }
      if (!hasMetaData.getAndSet(true)) {

        metricValueResult.addGroupBy(GroupByType.getNumberTypeInstance());
        if (tmpTags != null) {
          for (Map.Entry<String, Integer> entry : tag2pos.entrySet()) {
            pos2tag.put(entry.getValue(), entry.getKey());
          }
          for (Map.Entry<Integer, List<String>> entry : tmpTags.entrySet()) {
            metricValueResult.setTag(pos2tag.get(entry.getKey()), entry.getValue());
          }
        }
      }
    } catch (Exception e) {
      //TODO: add retry query, update connection if encountered error, retry once more
      LOGGER.error("{} Execute failed SQL: {}", Thread.currentThread().getName(), influxSql, e);
    }
  }


  private String getInfluxSql(String sql) {
    String[] sqlItem = sql.split(" ");
    StringBuilder influxSqlBuilder = new StringBuilder();
    for (int i = 0; i < 2; i++) {
      influxSqlBuilder.append(sqlItem[i]).append(" ");
    }
    influxSqlBuilder.append("FROM ");
    String[] splitDeviceId = sqlItem[3].split("\\.");
    StringBuilder noStorageGroupDeviceIdBuilder = new StringBuilder();
    for (int i = 2; i < splitDeviceId.length; i++) {
      noStorageGroupDeviceIdBuilder.append(splitDeviceId[i]).append(INFLUX_PATH_SPLIT);
    }
    noStorageGroupDeviceIdBuilder.deleteCharAt(noStorageGroupDeviceIdBuilder.length() - 1);
    influxSqlBuilder.append(noStorageGroupDeviceIdBuilder.toString()).append(" WHERE ");
    influxSqlBuilder.append(sqlItem[5]).append("ms AND ");
    influxSqlBuilder.append(sqlItem[7]).append("ms");
    return influxSqlBuilder.toString();
  }

  @Override
  public void createTimeSeries(Map<String, DataType> seriesPaths) {
    LOGGER.info("no need for InfluxDB to create TimeSeries");
  }

  @Override
  public void executeSQL(String sql) throws SQLException {
    // LOGGER.error("no implementation for InfluxDB executeSQL()");
  }

  @Override
  public void addSaveFromData(MetricValueResult valueResult, String path, String metricName)
      throws SQLException {
    LOGGER.error("no implementation for InfluxDB addSaveFromData()");
  }

  @Override
  public void deleteMetric(Map<String, Map<String, Integer>> tagOrder, String metricName)
      throws SQLException {
    LOGGER.error("no implementation for InfluxDB deleteMetric()");
  }

  @Override
  public void delete(String querySql) {
    LOGGER.error("no implementation for InfluxDB delete()");
  }

  @Override
  public long getValueResult(String sql, MetricValueResult metricValueResult) {
    LOGGER.error("no implementation for InfluxDB getValueResult()");
    return 0;
  }
}
