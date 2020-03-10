package cn.edu.tsinghua.iotdb.kairosdb.tsdb.influxdb;

import cn.edu.tsinghua.iotdb.kairosdb.http.rest.json.DataPointsParser.DataType;
import cn.edu.tsinghua.iotdb.kairosdb.query.QueryMetric;
import cn.edu.tsinghua.iotdb.kairosdb.query.result.MetricValueResult;
import cn.edu.tsinghua.iotdb.kairosdb.tsdb.IDatabase;
import cn.edu.tsinghua.iotdb.kairosdb.util.Util;
import java.sql.SQLException;
import java.util.HashMap;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Influx implements IDatabase {

  private static final Logger LOGGER = LoggerFactory.getLogger(Influx.class);
  private InfluxDB influxDbInstance;
  private static final String DB_NAME = "data";
  private static final String DEFAULT_RP = "autogen";
  private static final String SINGLE_MEASUREMENT = "group_0";
  private static final String TAG_KEY_NAME = "deviceId";
  private static final String TIMESTAMP_PRECISION = "ms";

  public Influx(String influxUrl) {
    try {
      OkHttpClient.Builder client = new Builder().connectTimeout(5, TimeUnit.MINUTES).
          readTimeout(5, TimeUnit.MINUTES).writeTimeout(5, TimeUnit.MINUTES).
          retryOnConnectionFailure(true);
      influxDbInstance = InfluxDBFactory.connect(influxUrl, client);
    } catch (Exception e) {
      LOGGER.error("Initialize InfluxDB failed because ", e);
    }
    try {
      assert influxDbInstance != null;
      influxDbInstance.createDatabase(DB_NAME);
    } catch (Exception e) {
      LOGGER.error("create InfluxDB database failed because ", e);
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
    // use single measurement
    model.setMeasurement(SINGLE_MEASUREMENT);
    HashMap<String, String> tags = new HashMap<>();
    tags.put(TAG_KEY_NAME, deviceId);
    model.setTagSet(tags);
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

  @Override
  public void rangeQuery(String sql, long metricCount, AtomicLong sampleSize,
      MetricValueResult metricValueResult, AtomicBoolean hasMetaData, QueryMetric metric) {
    
  }

  @Override
  public void createTimeSeries(Map<String, DataType> seriesPaths) {
    LOGGER.info("no need for InfluxDB to create TimeSeries");
  }

  @Override
  public void executeSQL(String sql) throws SQLException {

  }

  @Override
  public void addSaveFromData(MetricValueResult valueResult, String path, String metricName)
      throws SQLException {

  }

  @Override
  public void deleteMetric(Map<String, Map<String, Integer>> tagOrder, String metricName)
      throws SQLException {

  }

  @Override
  public void delete(String querySql) {

  }

  @Override
  public long getValueResult(String sql, MetricValueResult metricValueResult) {
    return 0;
  }
}
