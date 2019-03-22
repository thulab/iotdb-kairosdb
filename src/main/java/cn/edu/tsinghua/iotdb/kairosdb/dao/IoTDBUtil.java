package cn.edu.tsinghua.iotdb.kairosdb.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

public class IoTDBUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBUtil.class);

    private static final String URL ="jdbc:iotdb://%s:%s/";

    private static Connection connection;

    private IoTDBUtil() {}

    public static void initConnection(String host, String port, String user, String password) throws ClassNotFoundException, SQLException {
        Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
        connection = DriverManager.getConnection(String.format(URL, host, port), user, password);
    }

    public static Connection getConnection() { return connection; }

    public static void closeConnection() {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            LOGGER.error(e.getMessage());
        }
    }

    public static PreparedStatement getPreparedStatement(String sql, Object[] params) throws SQLException {
        PreparedStatement preparedStatement = connection.prepareStatement(sql);

        if (params != null)
            for (int i = 0; i < params.length; i++)
                preparedStatement.setObject(i + 1, params[i]);

        return preparedStatement;
    }

}
