package cn.edu.tsinghua.iotdb.kairosdb.dao;

import cn.edu.tsinghua.iotdb.kairosdb.conf.Config;
import cn.edu.tsinghua.iotdb.kairosdb.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.kairosdb.dao.IoTDBConnectionPool.ConnectionIterator;
import cn.edu.tsinghua.iotdb.kairosdb.query.result.MetricResult;
import cn.edu.tsinghua.iotdb.kairosdb.query.result.MetricValueResult;
import cn.edu.tsinghua.iotdb.kairosdb.query.result.QueryDataPoint;
import cn.edu.tsinghua.iotdb.kairosdb.rollup.RollUp;
import cn.edu.tsinghua.iotdb.kairosdb.rollup.RollUpException;
import cn.edu.tsinghua.iotdb.kairosdb.rollup.RollUpRecovery;
import cn.edu.tsinghua.iotdb.kairosdb.rollup.RollUpStoreImpl;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(MetricsManager.class);

  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  private static final String ERROR_OUTPUT_FORMATTER = "%s: %s";

  // The metadata maintained in the memory
  // metric - < tag, position>
  private static final Map<String, Map<String, Integer>> metric_tag_position = new ConcurrentHashMap<>();
  // metric - < tag, position>
  //private static final Map<String, Map<Integer, String>> metric_position_tag = new ConcurrentHashMap<>();
  private static final Map<String, List<String>> metric_tagNameOrder = new ConcurrentHashMap<>();


  // The SQL will be used to create metadata
  private static final String SYSTEM_CREATE_SQL = "CREATE TIMESERIES root.SYSTEM.TAG_NAME_INFO.%s WITH DATATYPE=%s, ENCODING=%s";
  private static final String METADATA_SERVICE_CREATE_SQL = "CREATE TIMESERIES root.SYSTEM.METADATA_SERVICE.%s WITH DATATYPE=%s, ENCODING=%s";

  // The SQL will be used to create rollup persistence data
  private static final String ROLLUP_CREATE_SQL = "CREATE TIMESERIES root.SYSTEM.ROLLUP.%s WITH DATATYPE=%s, ENCODING=%s";
  private static final String JSON = "json";

  // The constants of encoding methods
  private static final String TEXT_ENCODING = "PLAIN";
  private static final String INT64_ENCODING = "TS_2DIFF";
  private static final String INT32_ENCODING = "TS_2DIFF";
  private static final String DOUBLE_ENCODING = "GORILLA";

  // Storage group relevant config
  private static int storageGroupSize = config.STORAGE_GROUP_SIZE;
  private static final String STORAGE_GROUP_PREFIX = "group_";

  //index for persistence tag info in IoTDB
  private static AtomicLong index = new AtomicLong(1);

  private MetricsManager() {
  }

  /**
   * Load all of the metadata from database into memory. If the storage groups of metadata exist,
   * load out the content. If the storage groups of metadata don't exist, create all of the
   * TIMESERIES for persistent.
   */
  public static void loadMetadata(Connection connection) {
    LOGGER.info("Start loading system data.");
    Statement statement = null;
    Map<String, Map<Integer, String>> metric_position_tag = new ConcurrentHashMap<>();
    try {
      // Judge whether the TIMESERIES(root.SYSTEM.TAG_NAME_INFO) has been created
      statement = connection.createStatement();
      statement.execute(String.format("SHOW TIMESERIES root.%s", "SYSTEM"));
      ResultSet rs = statement.getResultSet();
      if (rs.next()) {
        /* Since the TIMESERIES are created
         * Recover the tag_key-potion mapping */
        statement = connection.createStatement();
        statement.execute(String
            .format("SELECT metric_name,tag_name,tag_order FROM %s", "root.SYSTEM.TAG_NAME_INFO"));
        rs = statement.getResultSet();
        long maxIndex = 0;
        while (rs.next()) {
          String name = rs.getString(2);
          String tagName = rs.getString(3);
          Integer pos = rs.getInt(4);
          if (metric_tag_position.containsKey(name)) {
            metric_tag_position.get(name).put(tagName, pos);
            metric_position_tag.get(name).put(pos, tagName);
          } else {
            Map<String, Integer> map = new HashMap<>();
            map.put(tagName, pos);
            metric_tag_position.put(name, map);
            Map<Integer, String> map2 = new HashMap<>();
            map2.put(pos, tagName);
            metric_position_tag.put(name, map2);

          }
          maxIndex = rs.getLong(1);
        }
        index.set(maxIndex + 1);

        // Read the size of storage group
        statement.execute(String.format("SELECT storage_group_size FROM %s",
            "root.SYSTEM.TAG_NAME_INFO"));
        rs = statement.getResultSet();
        if (rs.next()) {
          storageGroupSize = rs.getInt(2);
        } else {
          LOGGER.error("Database metadata has broken, use 30 as storage group size.");
//          System.exit(1);
          storageGroupSize = 30;
        }

        // Read the rollup tasks
        RollUpStoreImpl rollUpStore = new RollUpStoreImpl();

        try {
          Map<String, RollUp> historyTasks = rollUpStore.read();
          RollUpRecovery rollUpRecovery = new RollUpRecovery();
          rollUpRecovery.recover(historyTasks);
        } catch (RollUpException e) {
          LOGGER.error("Recover history rollup tasks failed because ", e);
        }
      } else {
        /* Since the TIMESERIES are not created
         * Create all the relevant TIMESERIES of metadata */
        statement.execute(String.format("SET STORAGE GROUP TO root.%s", "SYSTEM"));
        statement.execute(String.format(SYSTEM_CREATE_SQL, "metric_name", "TEXT", TEXT_ENCODING));
        statement.execute(String.format(SYSTEM_CREATE_SQL, "tag_name", "TEXT", TEXT_ENCODING));
        statement.execute(String.format(SYSTEM_CREATE_SQL, "tag_order", "INT32", INT32_ENCODING));

        statement
            .execute(String.format(METADATA_SERVICE_CREATE_SQL, "service", "TEXT", TEXT_ENCODING));
        statement.execute(
            String.format(METADATA_SERVICE_CREATE_SQL, "service_key", "TEXT", TEXT_ENCODING));
        statement.execute(String.format(METADATA_SERVICE_CREATE_SQL, "key", "TEXT", TEXT_ENCODING));
        statement.execute(
            String.format(METADATA_SERVICE_CREATE_SQL, "key_value", "TEXT", TEXT_ENCODING));

        // Initialize the storage group with STORAGE_GROUP_SIZE which is specified by config.properties
        statement.execute(
            String.format(SYSTEM_CREATE_SQL, "storage_group_size", "INT32", INT32_ENCODING));
        statement.execute(String.format(
            "insert into root.SYSTEM.TAG_NAME_INFO(timestamp, storage_group_size) values(%s, %s);",
            new Date().getTime(), storageGroupSize));
        for (int i = 0; i < storageGroupSize; i++) {
          statement
              .execute(String.format("SET STORAGE GROUP TO root.%s%s", STORAGE_GROUP_PREFIX, i));
        }

        // Create timeseries to persistence rollup tasks
        statement.execute(String.format(ROLLUP_CREATE_SQL, JSON, "TEXT", TEXT_ENCODING));
      }

    } catch (SQLException e) {
      LOGGER.error(String.format(ERROR_OUTPUT_FORMATTER, e.getClass().getName(), e.getMessage()), e);
    } finally {
      close(statement);
    }
    for (Map.Entry<String, Map<Integer, String>> entry : metric_position_tag.entrySet() ) {
      List<String> keyOrder = new ArrayList<>(entry.getValue().size());
      for (int i = 0 ; i < entry.getValue().size(); i++) {
        keyOrder.add(entry.getValue().get(i));
      }
      metric_tagNameOrder.put(entry.getKey(), keyOrder);
    }

    LOGGER.info("Finish loading system data.");

  }

  /**
   * Create a new TIMESERIES with given name, path and type.
   *
   * @param metricName The name of metric will be placed at the end of the path
   * @param path The path prefix of TIMESERIES
   * @param type The type of incoming data
   * @throws SQLException The exception will be throw when some errors occur while creating
   */
  private static void createNewMetric(String metricName, String path, String type)
      throws SQLException {
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
        encoding = TEXT_ENCODING;
    }
    ConnectionIterator iterator = IoTDBConnectionPool.getInstance().getConnectionIterator();
    while(iterator.hasNext()) {
      Connection connection = iterator.next();
      try (Statement statement = connection.createStatement()) {
        statement.execute(String
            .format(
                "CREATE TIMESERIES root.%s%s.%s WITH DATATYPE=%s, ENCODING=%s, COMPRESSOR=SNAPPY",
                getStorageGroupName(path), path, metricName, datatype, encoding));
      } finally {
        iterator.putBack(connection);
      }
    }
  }

  private static void createNewMetricAndIgnoreErrors(String metricName, String path, String type) {
    try {
      createNewMetric(metricName, path, type);
    } catch (SQLException ignore) {
      // ignore the exception
    }
  }

  public static void addDataPoints(MetricResult metric, String metricName) {
    ConnectionIterator iterator = IoTDBConnectionPool.getInstance().getConnectionIterator();
    while(iterator.hasNext()) {
      Connection connection = iterator.next();

      for (MetricValueResult valueResult : metric.getResults()) {
        if ((valueResult.isTextType() && metric.getResults().size() > 1)
            || valueResult.getDatapoints() == null
            || valueResult.getDatapoints().get(0) == null) {
          continue;
        }
        Map<String, String> tag = new HashMap<>();
        tag.put("saved_from", valueResult.getName());

        //HashMap<Integer, String> orderTagKeyMap = getNeededPosTagMap(metricName, tag);

        String path = getPath(metricName, tag);

        try (Statement statement = connection.createStatement()) {

          for (QueryDataPoint point : valueResult.getDatapoints()) {
            String insertingSql = String
                .format("insert into root.%s%s(timestamp,%s) values(%s,%s);",
                    getStorageGroupName(path),
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

          createNewMetricAndIgnoreErrors(metricName, path, type);

          statement.executeBatch();
        } catch (SQLException e) {
          LOGGER.error(String.format(ERROR_OUTPUT_FORMATTER, e.getClass().getName(), e.getMessage()));
        } finally {
          iterator.putBack(connection);
        }
      }
    }


  }

  public static void deleteMetric(String metricName) {

    ConnectionIterator iterator = IoTDBConnectionPool.getInstance().getConnectionIterator();
    while(iterator.hasNext()) {
      Connection connection = iterator.next();

      try (Statement statement = connection.createStatement()) {

        Map<String, Integer> mapping = metric_tag_position.getOrDefault(metricName, null);

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
          try {
            statement.execute(builder.toString());
          } catch (SQLException ignore) {
            // Ignore
          }
        }
        metric_tag_position.remove(metricName);
        metric_tagNameOrder.remove(metricName);
      } catch (SQLException e) {
        LOGGER.error(String.format(ERROR_OUTPUT_FORMATTER, e.getClass().getName(), e.getMessage()));
      } finally {
        iterator.putBack(connection);
      }
    }
  }


/*  *//**
   * Get or generate the mapping rule from position to tag_key of the given metric name and tags.
   *
   * @param name The metric name will be mapping
   * @param tagKeyValue The tags will be computed
   * @return The mapping rule from position to tag_key
   *//*
  public static HashMap<Integer, String> getNeededPosTagMap(String name, Map<String, String> tagKeyValue) {
    Map<String, Integer> tagKeyOrderMap = metric_tag_position.get(name);
    HashMap<Integer, String> mapping = new HashMap<>();
    HashMap<String, Integer> cache = new HashMap<>();
    if (null == tagKeyOrderMap) {
      // The metric name appears for the first time
      tagKeyOrderMap = new HashMap<>();
      Integer order = 0;
      for (String tagKey : tagKeyValue.keySet()) {
        tagKeyOrderMap.put(tagKey, order);
        mapping.put(order, tagKey);
        cache.put(tagKey, order);
        order++;
      }
      metric_tag_position.put(name, tagKeyOrderMap);
      metric_position_tag.put(name, mapping);
      persistMappingCache(name, cache);
    } else {
      // The metric name exists
      Map<Integer, String> posTagMap = metric_position_tag.get(name);
      for (Map.Entry<String, String> tag : tagKeyValue.entrySet()) {
        Integer pos = tagKeyOrderMap.get(tag.getKey());
        if (null == pos) {
          pos = tagKeyOrderMap.size();
          tagKeyOrderMap.put(tag.getKey(), pos);
          posTagMap.put(pos, tag.getKey());
          cache.put(tag.getKey(), pos);
          persistMappingCache(name, cache);
        }
        mapping.put(pos, tag.getKey());
      }
    }
    return mapping;
  }*/


  /**
   * for write.
   * @param name
   * @param tagKeyValues
   * @return 返回以 .开头的后缀串
   */
  public static String getPath(String name, Map<String, String> tagKeyValues) {
    StringBuilder stringBuilder = new StringBuilder();
    Map<String, String> copyTagKVs = new HashMap<>(tagKeyValues);

    List<String> keys = metric_tagNameOrder.get(name);

    if (keys != null) {
      for (String key : metric_tagNameOrder.get(name)) {
        if (copyTagKVs.containsKey(key)) {
          stringBuilder.append("." + copyTagKVs.remove(key));
        } else {
          stringBuilder.append(".d");
        }
      }
    }
    Map<String, Integer> tagPos;
    List<String> tagNameOrder;
    if (keys == null) {
      tagPos = new HashMap<>();
      tagNameOrder = new ArrayList<>();
      metric_tag_position.put(name,tagPos);
      metric_tagNameOrder.put(name, tagNameOrder);
    } else {
      tagPos = metric_tag_position.get(name);
      tagNameOrder = metric_tagNameOrder.get(name);
    }

    if (copyTagKVs.size() > 0) {
      int pos = tagNameOrder.size();
      Map<String, Integer> cache = new HashMap<>(copyTagKVs.size());
      for (String key : copyTagKVs.keySet()) {
        cache.put(key, pos);
        tagNameOrder.add(key);
        stringBuilder.append("." + copyTagKVs.get(key));
        pos ++;
      }
      persistMappingCache(name, cache);
    }
    return stringBuilder.toString();
  }


  /**
   * Persist the new mapping rule into database.
   *
   * @param metricName The name of the specific metric
   * @param cache The mapping cache will be persisted into database
   */
  private static void persistMappingCache(String metricName, Map<String, Integer> cache) {
    for (Map.Entry<String, Integer> entry : cache.entrySet()) {
      String sql = String.format(
          "insert into root.SYSTEM.TAG_NAME_INFO(timestamp, metric_name, tag_name, tag_order) values(%s, \"%s\", \"%s\", %s);",
          index.getAndIncrement(), metricName, entry.getKey(), entry.getValue());
      List<Connection> connections = null;


      ConnectionIterator iterator = IoTDBConnectionPool.getInstance().getConnectionIterator();
      while(iterator.hasNext()) {
        Connection connection = iterator.next();
        try (Statement statement = connection.createStatement()) {
          statement.execute(sql);
        } catch (SQLException e) {
          LOGGER
              .error(String.format(ERROR_OUTPUT_FORMATTER, e.getClass().getName(), e.getMessage()));
        } finally {
          iterator.putBack(connection);
        }
      }
    }
  }

  public static String generatePath(Map<String, String> tags,
      Map<Integer, String> orderTagKeyMap) {
    StringBuilder pathBuilder = new StringBuilder();
    int i = 0;
    int counter = 0;
    while (counter < tags.size()) {
      String path = tags.get(orderTagKeyMap.get(i));
      pathBuilder.append(".");
      if (null == path) {
        pathBuilder.append("d");
        counter++;
      } else {
        pathBuilder.append(path);
        counter++;
      }
      i++;
    }
    return pathBuilder.toString();
  }

  /**
   * Generate a corresponding storage group name of the given metric.
   *
   * @param path The device path of the specific metric
   * @return The corresponding storage group name of the given metric
   */
  public static String getStorageGroupName(String path) {
    if (path == null) {
      LOGGER.error(
          "MetricsManager.getStorageGroupName(String metricName): metricName could not be null.");
      return "null";
    }
    String device = path.split("\\.")[1];
//    int hashCode = 0;
//    if (device.length() == 4) {
//      hashCode = device.substring(0, 2).hashCode();
//    }else {
//      hashCode = device.substring(0, 1).hashCode();
//    }
//    int hashCode = path.split("\\.")[1].hashCode();
//    int protocal = 0;
    for (int i = 0; i < config.PROTOCAL_NUM; i++) {
      if (config.PROTOCAL_MACHINE.get(i).contains(device)) {
        return String.format("%s%s", STORAGE_GROUP_PREFIX, i);
      }
    }
    int hashCode = device.hashCode();
//    LOGGER.error("协议中不存在车辆{}", device);
    return String.format("%s%s", STORAGE_GROUP_PREFIX, Math.abs(hashCode) % storageGroupSize);
  }

  /**
   * Close the statement no matter whether it is open and ignore any exception.
   *
   * @param statement The statement will be closed
   */
  private static void close(Statement statement) {
    if (statement != null) {
      try {
        statement.close();
      } catch (SQLException e) {
        LOGGER.warn(String.format(ERROR_OUTPUT_FORMATTER, e.getClass().getName(), e.getMessage()));
      }
    }
  }

  public static Map<String, Integer> getTagPosMap(String metricName) {
    return metric_tag_position.getOrDefault(metricName, null);
  }

  public static boolean checkAllTagExisted(String metric, Set<String> keys) {
    Map<String, Integer> tags = metric_tag_position.get(metric);
    if (tags == null) return false;
    for (String key : keys) {
      if (!tags.containsKey(key)){
        return false;
      }
    }
    return true;
  }

  /**
   *
   * @param name
   * @return null 如果有tag不存在的话，或者metric不存在. 否则对tagKeys排序返回
   */
  public static String[] getPosTagList (String name) {
    /*Map<String, Integer> tagPosMap = metric_tag_position.get(name);
    if (tagPosMap == null || tagKeys.retainAll(tagPosMap.keySet())) {
      return null;
    }

    String[] result = new String[metric_position_tag.size()];

    Map<Integer, String> posTagMap = metric_position_tag.get(name);
    for (int i = 0; i < metric_position_tag.size(); i ++) {
      String tag = posTagMap.get(i);
      if (!tagKeys.contains(tag)) {
        result[i] = "*";
      } else {
        result[i] = tag;
      }
    }

    return result;*/
    return metric_tagNameOrder.get(name).toArray(new String[]{});
  }

  public static List<String> productPatch(List<String>[] tagValues) {
    return productPath(0, tagValues);
  }
  private static List<String> productPath(int currentLevel, List<String>[] tagValuesInEachLevel) {
    if (currentLevel == tagValuesInEachLevel.length -1 ) {
      List<String> result = new ArrayList<>();
      for (String tagValue : tagValuesInEachLevel[tagValuesInEachLevel.length -1]) {
        result.add("." + tagValue);
      }
      return result ;
    } else {
      List<String> result = new ArrayList<>();
      List<String> subPaths = productPath(currentLevel + 1, tagValuesInEachLevel);
      for (String tagValue : tagValuesInEachLevel[currentLevel]) {
        for (String subPath : subPaths) {
          result.add("." + tagValue +  subPath);
        }
      }
      return result;
    }
  }


  public static List<String> getMetricNamesList(String prefix) {
    if (prefix == null) {
      return new ArrayList<>(metric_tag_position.keySet());
    } else {
      List<String> list = new ArrayList<>();
      for (String name : metric_tag_position.keySet()) {
        if (name.startsWith(prefix)) {
          list.add(name);
        }
      }
      return list;
    }
  }

}
