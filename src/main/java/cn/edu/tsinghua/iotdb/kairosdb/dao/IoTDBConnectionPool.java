package cn.edu.tsinghua.iotdb.kairosdb.dao;

import cn.edu.tsinghua.iotdb.kairosdb.conf.Config;
import cn.edu.tsinghua.iotdb.kairosdb.conf.ConfigDescriptor;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IoTDBConnectionPool {

  private static final Config config = ConfigDescriptor.getInstance().getConfig();
  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBConnectionPool.class);
  public static final String CONNECT_STRING = "jdbc:iotdb://%s/";
  private AtomicInteger loop = new AtomicInteger(0);
  private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  private List<List<Connection>> connections_list = new ArrayList<>();

  private IoTDBConnectionPool() {
    createConnections();
  }

  public synchronized void createConnections() {
    lock.writeLock().lock();
    try {
      try {
        Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
      } catch (ClassNotFoundException e) {
        LOGGER.error("Class.forName(\"org.apache.iotdb.jdbc.IoTDBDriver\") failed ", e);
      }
      connections_list.clear();
      for (int j = 0; j < config.URL_LIST.size(); j++) {
        List<Connection> connections = new ArrayList<>();
        for (int i = 0; i < config.CONNECTION_NUM; i++) {
          try {
            Connection con = DriverManager
                .getConnection(String.format(CONNECT_STRING, config.URL_LIST.get(j)), "root",
                    "root");
            connections.add(con);
          } catch (SQLException e) {
            LOGGER.error("Get new connection failed ", e);
          }
        }
        connections_list.add(connections);
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  public List<Connection> getConnections() {
    lock.writeLock().lock();
    List<Connection> connections;
    try {
      if (loop.incrementAndGet() > config.CONNECTION_NUM * 10000) {
        loop.set(0);
      }
      connections = new ArrayList<>();
      for (int i = 0; i < config.URL_LIST.size(); i++) {
        connections.add(connections_list.get(i)
            .get(loop.getAndIncrement() % config.CONNECTION_NUM));
      }
    } finally {
      lock.writeLock().unlock();
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
