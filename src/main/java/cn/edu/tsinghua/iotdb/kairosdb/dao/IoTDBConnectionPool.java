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
  private static final String URL = "jdbc:iotdb://%s:%s/";
  private AtomicInteger loop = new AtomicInteger(0);

  private List<Connection> connections = new ArrayList<>();

  private IoTDBConnectionPool() {
    try {
      Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
    } catch (ClassNotFoundException e) {
      LOGGER.error("Class.forName(\"org.apache.iotdb.jdbc.IoTDBDriver\") failed ", e);
    }
    for (int i = 0; i < config.CONNECTION_NUM; i++) {
      try {
        Connection con = DriverManager
            .getConnection(String.format(URL, config.HOST, config.PORT), "root", "root");
        connections.add(con);
      } catch (SQLException e) {
        LOGGER.error("Get new connection failed ", e);
      }
    }
  }

  public Connection getConnection() {
    if (loop.incrementAndGet() > config.CONNECTION_NUM * 2) {
      loop.set(0);
    }
    return connections.get(loop.getAndIncrement() % config.CONNECTION_NUM);
  }

  private static class IoTDBConnectionPoolHolder {

    private static final IoTDBConnectionPool INSTANCE = new IoTDBConnectionPool();
  }

  public static IoTDBConnectionPool getInstance() {
    return IoTDBConnectionPoolHolder.INSTANCE;
  }

}
