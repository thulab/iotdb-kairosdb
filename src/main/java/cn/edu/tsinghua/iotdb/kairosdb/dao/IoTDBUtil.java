package cn.edu.tsinghua.iotdb.kairosdb.dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IoTDBUtil {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBUtil.class);

  private static final String URL = "jdbc:iotdb://%s:%s/";

  private static String host;
  private static String port;
  private static String user;
  private static String password;

  private static Connection connection;

  private IoTDBUtil() {
  }

  public static void initConnection(String host, String port, String user, String password)
      throws ClassNotFoundException, SQLException {
    IoTDBUtil.host = host;
    IoTDBUtil.port = port;
    IoTDBUtil.user = user;
    IoTDBUtil.password = password;
    Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
    connection = DriverManager.getConnection(String.format(URL, host, port), user, password);
  }

  public static Connection getNewConnection() throws SQLException, ClassNotFoundException {
    Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
    return DriverManager.getConnection(String.format(URL, host, port), user, password);
  }

  public static Connection getConnection() {
    return connection;
  }

  public static void closeConnection() {
    try {
      if (connection != null) {
        connection.close();
      }
    } catch (SQLException e) {
      LOGGER.error(e.getMessage());
    }
  }

  static PreparedStatement getPreparedStatement(String sql, Object[] params) throws SQLException {
    PreparedStatement preparedStatement = connection.prepareStatement(sql);

    if (params != null) {
      for (int i = 0; i < params.length; i++) {
        preparedStatement.setObject(i + 1, params[i]);
      }
    }

    return preparedStatement;
  }

}
