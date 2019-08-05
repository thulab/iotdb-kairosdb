package cn.edu.tsinghua.iotdb.kairosdb.dao;

import cn.edu.tsinghua.iotdb.kairosdb.metadata.MetadataException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetadataManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(MetadataManager.class);
  private static final String ERROR_OUTPUT_FORMATTER = "%s: %s";

  private static MetadataManager manager;

  private List<Connection> connections;

  private static final String NULL_STRING = "NULL";

  public static synchronized MetadataManager getInstance() {
    if (manager == null) {
      manager = new MetadataManager();
    }
    return manager;
  }

  private MetadataManager() {
    try {
      connections = IoTDBUtil.getNewConnection();
    } catch (SQLException | ClassNotFoundException e) {
      LOGGER.error(String.format(ERROR_OUTPUT_FORMATTER, e.getClass().getName(), e.getMessage()));
    }
  }

  public synchronized void addOrUpdateValue(
      String service, String serviceKey, String key, String value)
      throws MetadataException {
    for (Connection conn : connections) {
      try (Statement statement = conn.createStatement()) {
        statement.execute(new MetadataSqlGenerator().getQuerySql(service, serviceKey, key));
        ResultSet rs = statement.getResultSet();
        if (rs.next()) {
          statement.execute(new MetadataSqlGenerator().getUpdateSql(rs.getLong(1),
              service, serviceKey, key, value));
        } else {
          statement
              .execute(new MetadataSqlGenerator().getInsertSql(service, serviceKey, key, value));
        }
      } catch (SQLException e) {
        LOGGER.error(String.format(ERROR_OUTPUT_FORMATTER, e.getClass().getName(), e.getMessage()));
        throw new MetadataException("Failed to add value");
      }
    }
  }

  public String getValue(String service, String serviceKey, String key) {
    try (Statement statement = connections.get(0).createStatement()) {
      statement.execute(new MetadataSqlGenerator().getQuerySql(service, serviceKey, key));
      ResultSet rs = statement.getResultSet();
      if (rs.next()) {
        String value = rs.getString(2);
        if (value.equals(NULL_STRING)) {
          return null;
        }
        return value;
      }
    } catch (SQLException e) {
      LOGGER.error(String.format(ERROR_OUTPUT_FORMATTER, e.getClass().getName(), e.getMessage()));
    }
    return null;
  }

  public List<String> getServiceKeyList(String service) {
    List<String> list = new LinkedList<>();
    try (Statement statement = connections.get(0).createStatement()) {
      statement.execute(new MetadataSqlGenerator().getQuerySql(service));
      ResultSet rs = statement.getResultSet();
      while (rs.next()) {
        String serviceKey = rs.getString(2);
        if (list.contains(serviceKey)) {
          continue;
        }
        list.add(serviceKey);
      }
    } catch (SQLException e) {
      LOGGER.error(String.format(ERROR_OUTPUT_FORMATTER, e.getClass().getName(), e.getMessage()));
    }
    return list;
  }

  public List<String> getKeyList(String service, String serviceKey) {
    List<String> list = new LinkedList<>();
    try (Statement statement = connections.get(0).createStatement()) {
      statement.execute(new MetadataSqlGenerator().getQuerySql(service, serviceKey));
      ResultSet rs = statement.getResultSet();
      while (rs.next()) {
        String key = rs.getString(2);
        if (list.contains(key)) {
          continue;
        }
        list.add(key);
      }
    } catch (SQLException e) {
      LOGGER.error(String.format(ERROR_OUTPUT_FORMATTER, e.getClass().getName(), e.getMessage()));
    }
    return list;
  }

  public synchronized void deleteValue(String service, String serviceKey, String key)
      throws MetadataException {
    for (Connection conn : connections) {
      try (Statement statement = conn.createStatement()) {
        statement.execute(new MetadataSqlGenerator().getQuerySql(service, serviceKey, key));
        ResultSet rs = statement.getResultSet();
        if (rs.next()) {
          statement.execute(new MetadataSqlGenerator().getUpdateSql(rs.getLong(1),
              NULL_STRING, NULL_STRING, NULL_STRING, NULL_STRING));
        }
      } catch (SQLException e) {
        LOGGER.error(String.format(ERROR_OUTPUT_FORMATTER, e.getClass().getName(), e.getMessage()));
        throw new MetadataException("Failed to delete value");
      }
    }
  }

  private class MetadataSqlGenerator {

    MetadataSqlGenerator() {
    }

    String getInsertSql(String service, String serviceKey, String key, String value) {
      return getUpdateSql(getCurrentTimeStamp(), service, serviceKey, key, value);
    }

    String getUpdateSql(long timestamp, String service, String serviceKey, String key,
        String value) {
      return String.format("insert into root.SYSTEM.METADATA_SERVICE"
              + "(timestamp, service, service_key, key, key_value) "
              + "values(%s, \"%s\", \"%s\", \"%s\", \"%s\");",
          timestamp, service, serviceKey, key, value);
    }

    String getQuerySql(String service, String serviceKey, String key) {
      return String.format("SELECT key_value FROM root.SYSTEM.METADATA_SERVICE "
          + "WHERE service=\"%s\" and service_key=\"%s\" and key=\"%s\"", service, serviceKey, key);
    }

    String getQuerySql(String service) {
      return String.format("SELECT service_key FROM root.SYSTEM.METADATA_SERVICE "
          + "WHERE service=\"%s\"", service);
    }

    String getQuerySql(String service, String serviceKey) {
      return String.format("SELECT key FROM root.SYSTEM.METADATA_SERVICE "
          + "WHERE service=\"%s\" and service_key=\"%s\"", service, serviceKey);
    }

    private long getCurrentTimeStamp() {
      return new Date().getTime();
    }
  }

}
