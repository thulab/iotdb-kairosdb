package cn.edu.tsinghua.iotdb.kairosdb.tsdb.iotdb;

import cn.edu.tsinghua.iotdb.kairosdb.dao.MetricsManager;
import cn.edu.tsinghua.iotdb.kairosdb.http.rest.json.DataPointsParser.DataType;
import cn.edu.tsinghua.iotdb.kairosdb.query.result.MetricValueResult;
import cn.edu.tsinghua.iotdb.kairosdb.query.result.QueryDataPoint;
import cn.edu.tsinghua.iotdb.kairosdb.tsdb.IDatabase;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IoTDB implements IDatabase {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDB.class);
  private Connection connection;
  public static final String CONNECT_STRING = "jdbc:iotdb://%s/";
  // The constants of encoding methods
  private static final String TEXT_ENCODING = "PLAIN";
  private static final String INT64_ENCODING = "TS_2DIFF";
  private static final String DOUBLE_ENCODING = "GORILLA";

  public IoTDB(String url) {
    try {
      Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
    } catch (ClassNotFoundException e) {
      LOGGER.error("Class.forName(\"org.apache.iotdb.jdbc.IoTDBDriver\") failed ", e);
    }
    try {
      connection = DriverManager
          .getConnection(String.format(CONNECT_STRING, url), "root", "root");
    } catch (SQLException e) {
      LOGGER.error("Get new connection failed ", e);
    }
  }

  @Override
  public void insert(String deviceId, long timestamp, List<String> measurements,
      List<String> values) {

  }

  @Override
  public void rangeQuery() {

  }

  @Override
  public void createTimeSeries(Map<String, DataType> seriesPaths) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      for (Map.Entry<String, DataType> entry : seriesPaths.entrySet()) {
        try {
          statement.execute(createTimeSeriesSql(entry.getKey(), entry.getValue()));
        } catch (Exception e) {
          LOGGER.error("时间序列{}已存在", entry.getKey(), e);
        }
      }
    }
  }

  @Override
  public void executeSQL(String sql) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      statement.execute(sql);
    }
  }

  @Override
  public void addSaveFromData(MetricValueResult valueResult, String path, String metricName)
      throws SQLException {
    try(Statement statement = connection.createStatement()) {
      for (QueryDataPoint point : valueResult.getDatapoints()) {
        String insertingSql = String
            .format("insert into root.%s%s(timestamp,%s) values(%s,%s);",
                MetricsManager.getStorageGroupName(path),
                path, metricName, point.getTimestamp(), point.getAsString());
        statement.addBatch(insertingSql);
      }
      String type;
      switch (valueResult.getDatapoints().get(0).getType()) {
        case Types.INTEGER:
          type = "long";
          break;
        case Types.DOUBLE:
          type = "double";
          break;
        default:
          type = "text";
          break;
      }
      MetricsManager.createNewMetricAndIgnoreErrors(metricName, path, type);
      statement.executeBatch();
    }
  }

  @Override
  public void deleteMetric(
      Map<String, Map<String, Integer>> tagOrder, String metricName) throws SQLException {
    try(Statement statement = connection.createStatement()) {
      Map<String, Integer> mapping = tagOrder.getOrDefault(metricName, null);
      if (mapping == null) {
        return;
      }
      int size = mapping.size();
      for (int i = 0; i <= size; i++) {
        StringBuilder builder = new StringBuilder("DELETE TIMESERIES root.*");
        builder.append(".");
        for (int j = 0; j < i; j++) {
          builder.append("*.");
        }
        builder.append(metricName);
        MetricsManager.executeAndIgnoreException(statement, builder.toString());
      }
    }
  }

  private static String createTimeSeriesSql(String seriesPath, DataType type) {
    String datatype;
    String encoding;
    switch (type) {
      case LONG:
        datatype = "INT64";
        encoding = INT64_ENCODING;
        break;
      case DOUBLE:
        datatype = "DOUBLE";
        encoding = DOUBLE_ENCODING;
        break;
      default:
        datatype = "TEXT";
        encoding = TEXT_ENCODING;
    }
    return String
        .format("CREATE TIMESERIES %s WITH DATATYPE=%s, ENCODING=%s, COMPRESSOR=SNAPPY", seriesPath,
            datatype, encoding);
  }
}
