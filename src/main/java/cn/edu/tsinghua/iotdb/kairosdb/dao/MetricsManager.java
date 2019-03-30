package cn.edu.tsinghua.iotdb.kairosdb.dao;

import com.google.common.collect.ImmutableSortedMap;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.apache.iotdb.jdbc.IoTDBSQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(MetricsManager.class);
  private static final String ERROR_OUTPUT_FORMATTER = "%s: %s";

  private static final HashMap<String, HashMap<String, Integer>> tagOrder = new HashMap<>();
  private static final String SYSTEM_CREATE_SQL = "CREATE TIMESERIES root.SYSTEM.TAG_NAME_INFO.%s WITH DATATYPE=%s, ENCODING=%s, COMPRESSOR=SNAPPY";
  private static final String ENCODING_PLAIN = "PLAIN";
  private static int storageGroupSize = 10;
  private MetricsManager() {
  }

  public static void loadMetadata(int storageGroupSize) {
    if (storageGroupSize > 0) {
      MetricsManager.storageGroupSize = storageGroupSize;
    } else {
      LOGGER.warn("Storage group size must be larger than zero and has been set to default value({}).",
          storageGroupSize);
    }
    loadMetadata();
  }

  public static void loadMetadata() {
    LOGGER.info("Start loading system data.");
    Statement statement = null;
    ResultSet rs = null;
    try {
      statement = IoTDBUtil.getConnection().createStatement();
      statement.execute(String.format("SHOW TIMESERIES root.%s", "SYSTEM"));
      rs = statement.getResultSet();

      if (rs.next()) {
        statement = IoTDBUtil.getConnection().createStatement();
        statement.execute(String.format("SELECT metric_name,tag_name,tag_order FROM %s",
            "root.SYSTEM.TAG_NAME_INFO"));
        rs = statement.getResultSet();
        while (rs.next()) {
          String name = rs.getString(2);
          String tagName = rs.getString(3);
          Integer pos = rs.getInt(4);
          tagOrder.computeIfAbsent(name, k -> new HashMap<>());
          HashMap<String, Integer> temp = tagOrder.get(name);
          temp.put(tagName, pos);
        }
        statement.execute(String.format("SELECT storage_group_size FROM %s",
            "root.SYSTEM.TAG_NAME_INFO"));
        rs = statement.getResultSet();
        if (rs.next()) {
          storageGroupSize = rs.getInt(2);
        } else {
          LOGGER.error("Database metadata has broken, please reload a new database.");
          System.exit(1);
        }
      } else {
        statement.execute(String.format("SET STORAGE GROUP TO root.%s", "SYSTEM"));
        statement.execute(String.format(SYSTEM_CREATE_SQL, "metric_name", "TEXT", ENCODING_PLAIN));
        statement.execute(String.format(SYSTEM_CREATE_SQL, "tag_name", "TEXT", ENCODING_PLAIN));
        statement.execute(String.format(SYSTEM_CREATE_SQL, "tag_order", "INT32", "RLE"));
        statement.execute(String.format(SYSTEM_CREATE_SQL, "storage_group_size", "INT32", "RLE"));
        statement.execute(String.format(
            "insert into root.SYSTEM.TAG_NAME_INFO(timestamp, storage_group_size) values(%s, %s);",
            new Date().getTime(), storageGroupSize));
        for (int i = 0; i < storageGroupSize; i++) {
          statement.execute(String.format("SET STORAGE GROUP TO root.srg_%s", i));
        }
      }

    } catch (SQLException e) {
      LOGGER.error(String.format(ERROR_OUTPUT_FORMATTER, e.getClass().getName(), e.getMessage()));
    } finally {
      close(statement, rs);
    }
    LOGGER.info("Finish loading system data.");
  }

  private static void createNewMetric(String name, String type) throws SQLException {
    String datatype = "DOUBLE";
    String encoding = "GORILLA";
    if (type.equals("string")) {
      datatype = "TEXT";
      encoding = ENCODING_PLAIN;
    }
    Statement statement = IoTDBUtil.getConnection().createStatement();
    statement.execute(String
        .format("CREATE TIMESERIES root.%s%s WITH DATATYPE=%s, ENCODING=%s, COMPRESSOR=SNAPPY",
            getStorageGroupName(name), name, datatype, encoding));
    statement.close();
  }

  public static void addDatapoint(String name, ImmutableSortedMap<String, String> tags, String type,
      Long timestamp, String value) throws SQLException {

    if (null == tags) {
      return;
    }

    HashMap<Integer, String> mapping = getMapping(name, tags);
    HashMap<String, Integer> metricTags = tagOrder.get(name);

    if (type.equals("string")) {
      value = String.format("\"%s\"", value);
    }

    StringBuilder pathBuilder = new StringBuilder();
    int i = 0;
    int counter = 0;
    while (i < metricTags.size() && counter < tags.size()) {
      String path = tags.get(mapping.get(i));
      pathBuilder.append(".");
      if (null == path) {
        pathBuilder.append("d");
      } else {
        pathBuilder.append(path);
        counter++;
      }
      i++;
    }
    String insertingSql = String
        .format("insert into root.%s%s(timestamp,%s) values(%s,%s);", getStorageGroupName(name),
            pathBuilder.toString(), name, timestamp, value);

    PreparedStatement pst = null;
    ResultSet rs = null;
    try {
      pst = IoTDBUtil.getPreparedStatement(insertingSql, null);
      pst.executeUpdate();
      rs = pst.getResultSet();
    } catch (IoTDBSQLException e) {
      LOGGER.warn(String.format(ERROR_OUTPUT_FORMATTER, e.getClass().getName(), e.getMessage()));
      createNewMetric(String.format("%s.%s", pathBuilder.toString(), name), type);
      pst = IoTDBUtil.getPreparedStatement(insertingSql, null);
      pst.executeUpdate();
      rs = pst.getResultSet();
    } catch (SQLException e) {
      LOGGER.error("Add data points failed because ", e);
      throw e;
    } finally {
      close(pst, rs);
    }
  }

  private static HashMap<Integer, String> getMapping(String name, Map<String, String> tags) {
    HashMap<String, Integer> metricTags = tagOrder.get(name);
    HashMap<Integer, String> mapping = new HashMap<>();
    HashMap<String, Integer> cache = new HashMap<>();
    if (null == metricTags) {
      metricTags = new HashMap<>();
      Integer marker = 0;
      for (Map.Entry<String, String> tag : tags.entrySet()) {
        metricTags.put(tag.getKey(), marker);
        mapping.put(marker, tag.getKey());
        cache.put(tag.getKey(), marker);
        marker++;
      }
      tagOrder.put(name, metricTags);
    } else {
      for (Map.Entry<String, String> tag : tags.entrySet()) {
        Integer pos = metricTags.get(tag.getKey());
        if (null != pos) {
          mapping.put(pos, tag.getKey());
        } else {
          pos = metricTags.size();
          metricTags.put(tag.getKey(), pos);
          mapping.put(pos, tag.getKey());
          cache.put(tag.getKey(), pos);
        }
      }
    }
    persistMappingCache(name, cache);
    return mapping;
  }

  private static void persistMappingCache(String metricName, Map<String, Integer> cache) {
    for (Map.Entry<String, Integer> entry : cache.entrySet()) {
      long timestamp = new Date().getTime();
      String sql = String.format(
          "insert into root.SYSTEM.TAG_NAME_INFO(timestamp, metric_name, tag_name, tag_order) values(%s, \"%s\", \"%s\", %s);",
          timestamp, metricName, entry.getKey(), entry.getValue());
      try (Statement statement = IoTDBUtil.getConnection().createStatement()) {
        statement.execute(sql);
      } catch (SQLException e) {
        LOGGER.error(String.format(ERROR_OUTPUT_FORMATTER, e.getClass().getName(), e.getMessage()));
      }
    }
  }

  private static String getStorageGroupName(String metricName) {
    if (metricName == null) {
      LOGGER.error("MetricsManager.getStorageGroupName(String metricName): metricName could not be null.");
      return "srg_null";
    }
    int hashCode = metricName.hashCode();
    return String.format("srg_%s", Math.abs(hashCode) % storageGroupSize);
  }

  private static void close(Statement statement, ResultSet resultSet) {
    if (resultSet != null) {
      try {
        resultSet.close();
      } catch (SQLException e) {
        LOGGER.warn(
            String.format(ERROR_OUTPUT_FORMATTER, e.getClass().getName(), e.getMessage()));
      }
    }
    if (statement != null)
      try {
        statement.close();
      } catch (SQLException e) {
        LOGGER.warn(String.format(ERROR_OUTPUT_FORMATTER, e.getClass().getName(), e.getMessage()));
      }
  }

}
