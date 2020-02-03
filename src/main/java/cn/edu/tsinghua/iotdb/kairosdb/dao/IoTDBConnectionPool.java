package cn.edu.tsinghua.iotdb.kairosdb.dao;

import cn.edu.tsinghua.iotdb.kairosdb.conf.Config;
import cn.edu.tsinghua.iotdb.kairosdb.conf.ConfigDescriptor;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IoTDBConnectionPool {

  private static final Config config = ConfigDescriptor.getInstance().getConfig();
  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBConnectionPool.class);
  public static final String CONNECT_STRING = "jdbc:iotdb://%s/";
  private AtomicInteger loop = new AtomicInteger(0);
  private AtomicInteger readOnlyLoop = new AtomicInteger(0);
  private List<List<Connection>> writeReadConnectionsList = new ArrayList<>();
  private List<List<List<Connection>>> readOnlyConnectionsList = new ArrayList<>();

  // lock-less by using AtomicInteger
  public Connection getReadOnlyConnection(int zone, String schema) {
    if (readOnlyLoop.incrementAndGet() > config.CONNECTION_NUM * 1000) {
      readOnlyLoop.set(0);
    }
    // use schema's hash code because we want use the query cache in IoTDB
    if(schema == null) {
      schema = "d";
    }
    int sameZoneIndex = Math.abs(schema.hashCode() % config.IoTDB_READ_ONLY_LIST.get(zone).size());
    return readOnlyConnectionsList.get(zone).get(sameZoneIndex)
        .get(readOnlyLoop.getAndIncrement() % config.CONNECTION_NUM);
  }

  private IoTDBConnectionPool() {
    createConnections();
  }

  public synchronized void createConnections() {
    try {
      Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
    } catch (ClassNotFoundException e) {
      LOGGER.error("Class.forName(\"org.apache.iotdb.jdbc.IoTDBDriver\") failed ", e);
    }
    writeReadConnectionsList.clear();
    for (int j = 0; j < config.IoTDB_LIST.size(); j++) {
      List<Connection> connections = new ArrayList<>();
      for (int i = 0; i < config.CONNECTION_NUM; i++) {
        try {
          Connection con = DriverManager
              .getConnection(String.format(CONNECT_STRING, config.IoTDB_LIST.get(j)), "root",
                  "root");
          connections.add(con);
        } catch (SQLException e) {
          LOGGER.error("Get new connection failed ", e);
        }
      }
      writeReadConnectionsList.add(connections);
    }

    readOnlyConnectionsList.clear();
    for (int i = 0; i < config.IoTDB_READ_ONLY_LIST.size(); i++) {
      List<List<Connection>> connections = new ArrayList<>();
      for (int j = 0; j < config.IoTDB_READ_ONLY_LIST.get(i).size(); j++) {
        List<Connection> sameInstanceConnections = new ArrayList<>();
        for (int k = 0; k < config.CONNECTION_NUM; k++) {
          try {
            Connection con = DriverManager
                .getConnection(String.format(CONNECT_STRING,
                    config.IoTDB_READ_ONLY_LIST.get(i).get(j)),
                    "root",
                    "root");
            sameInstanceConnections.add(con);
          } catch (SQLException e) {
            LOGGER.error("Get new connection failed ", e);
          }
        }
        connections.add(sameInstanceConnections);
      }
      readOnlyConnectionsList.add(connections);
    }
  }

  public List<Connection> getConnections() {
    List<Connection> connections;
    if (loop.incrementAndGet() > config.CONNECTION_NUM * 10000) {
      loop.set(0);
    }
    connections = new ArrayList<>();
    for (int i = 0; i < config.IoTDB_LIST.size(); i++) {
      connections.add(writeReadConnectionsList.get(i)
          .get(loop.getAndIncrement() % config.CONNECTION_NUM));
    }
    return connections;
  }

  private static class IoTDBConnectionPoolHolder {
    private static final IoTDBConnectionPool INSTANCE = new IoTDBConnectionPool();
  }

  public static IoTDBConnectionPool getInstance() {
    return IoTDBConnectionPoolHolder.INSTANCE;
  }

}
