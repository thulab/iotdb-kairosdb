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

  private static final HashMap<String, Map<String, Integer>> tagOrder = new HashMap<>();
  private static final String SYSTEM_CREATE_SQL = "CREATE TIMESERIES root.SYSTEM.TAG_NAME_INFO.%s WITH DATATYPE=%s, ENCODING=%s";
  private static final String ENCODING_PLAIN = "PLAIN";
  private static final String INT64_ENCODING = "TS_2DIFF";
  private static final String INT32_ENCODING = "TS_2DIFF";
  private static final String DOUBLE_ENCODING = "GORILLA";
  private static String storageGroup = "default";

  private MetricsManager() {
  }

  public static void loadMetadata(String storageGroup) {
    if (null != storageGroup) {
      MetricsManager.storageGroup = storageGroup;
    }
    loadMetadata();
  }

  private static void loadMetadata() {
    LOGGER.info("Start loading system data.");
    Statement statement = null;
    ResultSet rs = null;
    try {
      statement = IoTDBUtil.getConnection().createStatement();
      statement.execute(String.format("SHOW TIMESERIES root.%s", "SYSTEM"));
      rs = statement.getResultSet();

      if (rs.next()) {
        statement = IoTDBUtil.getConnection().createStatement();
        statement.execute(String.format("SELECT * FROM %s", "root.SYSTEM.TAG_NAME_INFO"));
        rs = statement.getResultSet();
        while (rs.next()) {
          String name = rs.getString(2);
          String tagName = rs.getString(3);
          Integer pos = rs.getInt(4);
          Map<String, Integer> temp = tagOrder.get(name);
          if (null == temp) {
            temp = new HashMap<>();
            tagOrder.put(name, temp);
          }
          temp.put(tagName, pos);
        }
      } else {
        statement.execute(String.format("SET STORAGE GROUP TO root.%s", "SYSTEM"));
        statement.execute(String.format(SYSTEM_CREATE_SQL, "metric_name", "TEXT", ENCODING_PLAIN));
        statement.execute(String.format(SYSTEM_CREATE_SQL, "tag_name", "TEXT", ENCODING_PLAIN));
        statement.execute(String.format(SYSTEM_CREATE_SQL, "tag_order", "INT32", INT32_ENCODING));
        statement.execute(String.format("SET STORAGE GROUP TO root.%s", storageGroup));
      }

    } catch (SQLException e) {
      LOGGER.error(String.format(ERROR_OUTPUT_FORMATTER, e.getClass().getName(), e.getMessage()));
    } finally {
      close(statement);
    }
    LOGGER.info("Finish loading system data.");
  }

  private static void createNewMetric(String name, String type) throws SQLException {
    String datatype;
    String encoding;
    switch (type) {
      case "long":
        datatype = "INT64";
        encoding = INT64_ENCODING;
        break;
      case "double":
        datatype = "DOUBLE";
        encoding = DOUBLE_ENCODING;
        break;
      default:
        datatype = "TEXT";
        encoding = ENCODING_PLAIN;
    }
    try (Statement statement = IoTDBUtil.getConnection().createStatement()) {
      statement.execute(String
          .format("CREATE TIMESERIES root.%s%s WITH DATATYPE=%s, ENCODING=%s, COMPRESSOR=SNAPPY",
              storageGroup, name, datatype, encoding));
    }
  }

  public static void addDatapoint(String name, ImmutableSortedMap<String, String> tags, String type,
      Long timestamp, String value) throws SQLException {

    if (null == tags) {
      LOGGER.error("metric {} have no tag", name);
      return;
    }

    HashMap<Integer, String> orderTagKeyMap = getMapping(name, tags);
    Map<String, Integer> metricTags = tagOrder.get(name);

    if (type.equals("string")) {
      value = String.format("\"%s\"", value);
    }

    StringBuilder pathBuilder = new StringBuilder();
    int i = 0;
    int counter = 0;
    while (i < metricTags.size() && counter < tags.size()) {
      String path = tags.get(orderTagKeyMap.get(i));
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
        .format("insert into root.%s%s(timestamp,%s) values(%s,%s);", storageGroup,
            pathBuilder.toString(), name, timestamp, value);

    PreparedStatement pst = null;
    try {
      pst = IoTDBUtil.getPreparedStatement(insertingSql, null);
      pst.executeUpdate();
    } catch (IoTDBSQLException e) {
      LOGGER.warn(String.format(ERROR_OUTPUT_FORMATTER, e.getClass().getName(), e.getMessage()));
      createNewMetric(String.format("%s.%s", pathBuilder.toString(), name), type);
      pst = IoTDBUtil.getPreparedStatement(insertingSql, null);
      pst.executeUpdate();
    } catch (SQLException e) {
      LOGGER.error("Add data points failed because ", e);
      throw e;
    } finally {
      close(pst);
    }
  }

  private static HashMap<Integer, String> getMapping(String name, Map<String, String> tags) {
    Map<String, Integer> tagKeyOrderMap = tagOrder.get(name);
    HashMap<Integer, String> mapping = new HashMap<>();
    HashMap<String, Integer> cache = new HashMap<>();
    if (null == tagKeyOrderMap) {
      tagKeyOrderMap = new HashMap<>();
      Integer order = 0;
      for (String tagKey : tags.keySet()) {
        tagKeyOrderMap.put(tagKey, order);
        mapping.put(order, tagKey);
        cache.put(tagKey, order);
        order++;
      }
      tagOrder.put(name, tagKeyOrderMap);
      persistMappingCache(name, cache);
    } else {
      for (Map.Entry<String, String> tag : tags.entrySet()) {
        Integer pos = tagKeyOrderMap.get(tag.getKey());
        if (null == pos) {
          pos = tagKeyOrderMap.size();
          tagKeyOrderMap.put(tag.getKey(), pos);
          cache.put(tag.getKey(), pos);
          persistMappingCache(name, cache);
        }
        mapping.put(pos, tag.getKey());
      }
    }
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

  private static void close(Statement statement) {
    if (statement != null) {
      try {
        statement.close();
      } catch (SQLException e) {
        LOGGER.warn(String.format(ERROR_OUTPUT_FORMATTER, e.getClass().getName(), e.getMessage()));
      }
    }
  }

}
