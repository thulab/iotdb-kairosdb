package cn.edu.tsinghua;

import cn.edu.tsinghua.iotdb.kairosdb.dao.IoTDBUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

public class IoTDBConnectorTestCase {
    private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBConnectorTestCase.class);

    private static final String URL = "jdbc:iotdb://%s:%s/";
    private static final String host = "127.0.0.1";
    private static final String port = "6667";
    private static final String USER = "root";
    private static final String PASSWORD = "root";

    private static Connection connection;



    private static void init() throws SQLException, ClassNotFoundException {
        Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
        connection = DriverManager.getConnection(String.format(URL, host, port), USER, PASSWORD);
    }

    private static void close() throws SQLException {
        if (connection != null)
            connection.close();
    }

    public static void main(String[] argv) {
        LOGGER.info("Starting IoTDB Connector test cases.");
        try {
            init();
            LOGGER.info("Success in initialing the connection to IoTDB");

//            ResultSet rs = executeQuery("select s0,s1 from root.vehicle.d0", null);
//            Statement state = connection.createStatement();
//            state.execute("SHOW TIMESERIES root");
//            ResultSet rs = state.getResultSet();

            Statement statement = connection.createStatement();
            statement.execute("SELECT * FROM root.SYSTEM.TAG_NAME_INFO");
            ResultSet rs = statement.getResultSet();

            ResultSetMetaData rsmd = rs.getMetaData();
//            for (int i = 1; i <= rsmd.getColumnCount(); i++) {
//                System.out.print(rsmd.getColumnLabel(i)+ "  ");
//                System.out.print(rsmd.getColumnName(i) + "  ");
//                System.out.print(rsmd.getColumnType(i) + "  ");
//                System.out.print(rsmd.getSchemaName(i) + "  ");
//                System.out.println();
//            }
//            if (rs.next()){
//                System.out.println(true);
//            } else {
//                System.out.println(false);
//            }

            while (rs.next()) {
                StringBuilder builder = new StringBuilder();
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    builder.append(rs.getString(i)).append("\t");
                }
                System.out.println(builder);
            }


            close();
            LOGGER.info("Success in closing the connection to IoTDB");
        } catch (ClassNotFoundException e) {
            LOGGER.error("Driver Class Not Found!");
        } catch (SQLException e) {
            LOGGER.error(e.getClass().getName() + ": " + e.getMessage());
        }
    }

    private static int executeUpdate(String sql, Object[] params) {
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(sql);

            if (params != null)
                for (int i = 0; i < params.length; i++)
                    preparedStatement.setObject(i + 1, params[i]);

            return preparedStatement.executeUpdate();
        } catch (SQLException e) {
            LOGGER.error(e.getMessage());
        }
        return 0;
    }

    private static ResultSet executeQuery(String sql, Object[] params) {
        ResultSet rst = null;
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(sql);

            if (params != null)
                for (int i = 0; i < params.length; i++)
                    preparedStatement.setObject(i + 1, params[i]);

            rst = preparedStatement.executeQuery();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return rst;
    }

}
