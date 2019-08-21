/*
package cn.edu.tsinghua.iotdb.kairosdb.dao;

import cn.edu.tsinghua.iotdb.kairosdb.conf.ConfigDescriptor;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IoTDBUtil {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBUtil.class);

  //  private static final String URL = "jdbc:iotdb://%s:%s/";
  private static final String CONNECT_String = "jdbc:iotdb://%s/";

  //  private static String host;
  //  private static String port;
  //  private static String URL;
  private final static String user = "root";
  private final static String password = "root";

  private static List<Connection> connections = new ArrayList<>();

  private IoTDBUtil() {
  }

//  public static void initConnection(String url, String user, String password)
//      throws ClassNotFoundException, SQLException {
//    IoTDBUtil.host = host;
//    IoTDBUtil.port = port;
//    IoTDBUtil.URL = url;
//    IoTDBUtil.user = user;
//    IoTDBUtil.password = password;
//    Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
//    connection = DriverManagererManager.getConnection(String.format(CONNECT_String, URL), user, password);
//  }

  public static List<Connection> getNewConnection() throws SQLException, ClassNotFoundException {
    Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
    List<Connection> connections = new ArrayList<>();
    for (String url : ConfigDescriptor.getInstance().getConfig().URL_LIST) {
      connections
          .add(DriverManager.getConnection(String.format(CONNECT_String, url), user, password));
    }
    return connections;
  }

  public static Connection getConnection(String url, String user, String password)
      throws ClassNotFoundException, SQLException {
    Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
    return DriverManager.getConnection(String.format(CONNECT_String, url), user, password);
  }

  public static List<Connection> getConnection() throws SQLException, ClassNotFoundException {
    return getNewConnection();
  }

  public static void closeConnection() {
    try {
      for (Connection conn : connections) {
        if (conn != null) {
          conn.close();
        }
      }
    } catch (SQLException e) {
      LOGGER.error(e.getMessage());
    }
  }

}
*/
