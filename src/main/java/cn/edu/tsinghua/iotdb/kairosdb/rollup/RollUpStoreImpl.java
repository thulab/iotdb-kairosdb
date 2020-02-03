package cn.edu.tsinghua.iotdb.kairosdb.rollup;

import cn.edu.tsinghua.iotdb.kairosdb.conf.Config;
import cn.edu.tsinghua.iotdb.kairosdb.conf.ConfigDescriptor;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RollUpStoreImpl implements RollUpStore {

  private static final Config config = ConfigDescriptor.getInstance().getConfig();
  private static final Logger LOGGER = LoggerFactory.getLogger(RollUpStoreImpl.class);
  private static final String CONNECT_STRING = "jdbc:iotdb://%s/";
  private static List<Connection> connections = new ArrayList<>();
  private static final String USER = "root";
  private static final String PWD = "root";
  private RollUpParser parser = new RollUpParser();


  public RollUpStoreImpl() {
    try {
      Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
      for (String url : config.IoTDB_LIST) {
        connections.add(DriverManager
            .getConnection(String.format(CONNECT_STRING, url), USER, PWD));
      }
    } catch (Exception e) {
      LOGGER.error("Initialize RollUpStoreImpl IoTDB connection failed because ", e);
    }
  }

  @Override
  public void write(String rollUpJson, String id) throws RollUpException {
    for (Connection conn : connections) {
      try (Statement statement = conn.createStatement()) {
        statement.execute(String.format(
            "insert into root.SYSTEM.ROLLUP(timestamp, json) values(%s, %s);", id,
            "'" + rollUpJson + "'"));
      } catch (SQLException e) {
        LOGGER.error("Write rollup JSON to IoTDB failed because ", e);
        throw new RollUpException(e);
      }
    }
  }

  @Override
  public Map<String, RollUp> read() throws RollUpException {
    Map<String, RollUp> allTasks = new HashMap<>();
    try (Statement statement = connections.get(0).createStatement()) {
      // Read the rollup tasks
      statement.execute(String.format("SELECT %s FROM %s", "json", "root.SYSTEM.ROLLUP"));
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String id = resultSet.getString(1);
          String json = resultSet.getString(2);
          if (!json.equals("NULL")) {
            RollUp rollUp = parser.parseRollupTask(json, id);
            allTasks.put(id, rollUp);
          }
        }
      }
    } catch (Exception e) {
      throw new RollUpException(e);
    }
    return allTasks;
  }

  @Override
  public void remove(String id) throws RollUpException {
    for (Connection conn : connections) {
      try (Statement statement = conn.createStatement()) {
        statement.execute(String.format(
            "insert into root.SYSTEM.ROLLUP(timestamp, json) values(%s, %s);", id, "\"NULL\""));
      } catch (Exception e) {
        throw new RollUpException(e);
      }
    }
  }

  @Override
  public RollUp read(String id) throws RollUpException {
    RollUp rollUp = null;
    try (Statement statement = connections.get(0).createStatement()) {
      // Read the rollup tasks
      statement.execute(
          String.format("SELECT %s FROM %s WHERE time = %s ", "json", "root.SYSTEM.ROLLUP", id));
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String json = resultSet.getString(2);
          if (!json.equals("NULL")) {
            rollUp = parser.parseRollupTask(json, id);
          }
        }
      }
    } catch (Exception e) {
      throw new RollUpException(e);
    }
    return rollUp;
  }

}
