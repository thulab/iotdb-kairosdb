package cn.edu.tsinghua.iotdb.kairosdb.dao;

import cn.edu.tsinghua.iotdb.kairosdb.conf.Config;
import cn.edu.tsinghua.iotdb.kairosdb.conf.ConfigDescriptor;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IoTDBConnectionPool {

  private static final Config config = ConfigDescriptor.getInstance().getConfig();
  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBConnectionPool.class);
  private static final String CONNECT_STRING = "jdbc:iotdb://%s/";
  private AtomicInteger loop = new AtomicInteger(0);

  static final Logger logger = LoggerFactory.getLogger(IoTDBConnectionPool.class);

  private LinkedList<Connection>[] connections_list;

  private IoTDBConnectionPool() {
    try {
      Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
    } catch (ClassNotFoundException e) {
      LOGGER.error("Class.forName(\"org.apache.iotdb.jdbc.IoTDBDriver\") failed ", e);
    }
    connections_list = new LinkedList[config.URL_LIST.size()];

    for (int j = 0; j < config.URL_LIST.size(); j++) {
      LinkedList<Connection> connections = new LinkedList<>();
      for (int i = 0; i < config.CONNECTION_NUM; i++) {
        try {
          Connection con = DriverManager
              .getConnection(String.format(CONNECT_STRING, config.URL_LIST.get(j)), "root", "root");
          connections.add(con);
        } catch (SQLException e) {
          LOGGER.error("Get new connection failed ", e);
        }
      }
      connections_list[j] = connections;
    }
  }

  public ConnectionIterator getConnectionIterator() {
    return new ConnectionIterator();
  }

//  /**
//   * get connections from pool of each IoTDB instance.
//   * @return
//   */
//  @Deprecated
//  public List<Connection> getConnections() {
//    if (loop.incrementAndGet() > config.CONNECTION_NUM * 2) {
//      loop.set(0);
//    }
//    List<Connection> connections = new ArrayList<>();
//    for (int i = 0; i < config.URL_LIST.size(); i++) {
//      connections.add(connections_list[i]
//          .get(loop.getAndIncrement() % config.CONNECTION_NUM));
//    }
//    return connections;
//  }

  private static class IoTDBConnectionPoolHolder {

    private static final IoTDBConnectionPool INSTANCE = new IoTDBConnectionPool();
  }

  public static IoTDBConnectionPool getInstance() {
    return IoTDBConnectionPoolHolder.INSTANCE;
  }

  public void closeAllConnections() {
    for (LinkedList<Connection> connections : connections_list) {
      for (Connection connection : connections) {
        try {
          connection.close();
        } catch (SQLException e) {
          logger.error(e.getMessage());
        }
      }
    }
  }

  /**
   * just for test
   * @param url
   * @param user
   * @param password
   * @return
   * @throws ClassNotFoundException
   * @throws SQLException
   */
  public static Connection getConnection(String url, String user, String password)
      throws ClassNotFoundException, SQLException {
    Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
    return DriverManager.getConnection(String.format(CONNECT_STRING, url), user, password);
  }

  /**
   * do not be maintained by the pool
   * @param url
   * @return
   * @throws ClassNotFoundException
   * @throws SQLException
   */
  public static Connection getConnection(String url)
      throws ClassNotFoundException, SQLException {
    Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
    return DriverManager.getConnection(String.format(CONNECT_STRING, url), "root", "root");
  }



  public  class ConnectionIterator implements Iterator<Connection> {
    int loc = 0;

    @Override
    public boolean hasNext() {
      return (loc < connections_list.length);
    }

    @Override
    public Connection next() {
      Connection result;
      synchronized (connections_list[loc]) {
        if (connections_list[loc].size() == 0) {
          try {
            logger.info("no available connection in {}, waiting...", config.URL_LIST.get(loc));
            connections_list[loc].wait();
            logger.info("get one connection in {}, end waiting", config.URL_LIST.get(loc));
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
        result =  connections_list[loc].pollFirst();
      }
      return result;
    }

    public void putBack(Connection connection) {
      synchronized (connections_list[loc]) {
        connections_list[loc].push(connection);
        connections_list[loc].notifyAll();
      }
      loc ++;
    }
  }

}
