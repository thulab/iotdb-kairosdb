package cn.edu.tsinghua.iotdb.kairosdb.tsdb.iotdb;

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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IoTDB implements IDatabase {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDB.class);
  private static final Config config = ConfigDescriptor.getInstance().getConfig();
  private Connection connection;
  public static final String CONNECT_STRING = "jdbc:iotdb://%s/";
  // The constants of encoding methods
  private static final String TEXT_ENCODING = "PLAIN";
  private static final String INT64_ENCODING = "TS_2DIFF";
  private static final String DOUBLE_ENCODING = "GORILLA";
  private Map<Integer, List<String>> tmpTags;
  private Map<String, Integer> tag2pos;
  private Map<Integer, String> pos2tag;

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

  private boolean isNumeric(String string) {
    for (int i = 0; i < string.length(); i++) {
      if (!Character.isDigit(string.charAt(i))) {
        return false;
      }
    }
    return true;
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

  @Override
  public void insert(String deviceId, long timestamp, List<String> measurements,
      List<String> values) {

  }

  @Override
  public void rangeQuery(String sql, long metricCount, AtomicLong sampleSize,
      MetricValueResult metricValueResult, AtomicBoolean hasMetaData, QueryMetric metric) {
    if (!getMetricMapping(metric)) {
      LOGGER.error("Get metric mapping of {} failed!", metric);
    }
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

  private List<String> buildDeleteSql(ResultSet rs) throws SQLException {
    ResultSetMetaData metaData = rs.getMetaData();

    String[] paths = new String[metaData.getColumnCount() - 1];
    int[] types = new int[metaData.getColumnCount() - 1];

    for (int i = 2; i <= metaData.getColumnCount(); i++) {
      paths[i - 2] = metaData.getColumnName(i);
      types[i - 2] = metaData.getColumnType(i);
    }

    DeleteSqlBuilder builder;
    builder = new DeleteSqlBuilder();

    while (rs.next()) {
      String timestamp = rs.getString(1);
      for (int i = 2; i <= metaData.getColumnCount(); i++) {
        if (rs.getString(i) != null) {
          builder.appendDataPoint(paths[i - 2], timestamp);
        }
      }
    }

    return builder.build(paths, types);
  }

  @Override
  public void delete(String querySql) {
    try {
      Statement statement = connection.createStatement();
      statement.execute(querySql);
      ResultSet rs = statement.getResultSet();
      List<String> sqlList = buildDeleteSql(rs);
      statement = connection.createStatement();
      for (String sql : sqlList) {
        statement.addBatch(sql);
      }
      statement.executeBatch();
    } catch (SQLException e) {
      LOGGER.error(String.format("%s: %s", e.getClass().getName(), e.getMessage()));
    }
  }

  @Override
  public long getValueResult(String sql, MetricValueResult metricValueResult) {
    long start = 0;
    if (config.ENABLE_PROFILER) {
      start = System.nanoTime();
    }
    long sampleSize = 0;
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
      LOGGER
          .warn(String.format("QueryExecutor.%s: %s", e.getClass().getName(), e.getMessage()));
    }
    return sampleSize;
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
