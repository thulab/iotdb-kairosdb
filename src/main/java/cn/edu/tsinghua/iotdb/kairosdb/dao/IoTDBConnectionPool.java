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
  private static final String CONNECT_STRING = "jdbc:iotdb://%s/";
  private AtomicInteger loop = new AtomicInteger(0);

  private List<List<Connection>> connections_list = new ArrayList<>();

  private IoTDBConnectionPool() {
    try {
      Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
    } catch (ClassNotFoundException e) {
      LOGGER.error("Class.forName(\"org.apache.iotdb.jdbc.IoTDBDriver\") failed ", e);
    }
    for (int j = 0; j < config.URL_LIST.size(); j++) {
      List<Connection> connections = new ArrayList<>();
      for (int i = 0; i < config.CONNECTION_NUM; i++) {
        try {
          Connection con = DriverManager
              .getConnection(String.format(CONNECT_STRING, config.URL_LIST.get(j)), "root", "root");
          connections.add(con);
        } catch (SQLException e) {
          LOGGER.error("Get new connection failed ", e);
        }
      }
      connections_list.add(connections);
    }
  }

  public List<Connection> getConnections() {
    if (loop.incrementAndGet() > config.CONNECTION_NUM * 2) {
      loop.set(0);
    }
    List<Connection> connections = new ArrayList<>();
    for (int i = 0; i < config.URL_LIST.size(); i++) {
      connections.add(connections_list.get(i)
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
