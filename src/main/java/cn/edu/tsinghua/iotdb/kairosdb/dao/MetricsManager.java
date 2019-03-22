package cn.edu.tsinghua.iotdb.kairosdb.dao;

import com.google.common.collect.ImmutableSortedMap;
import com.google.gson.JsonElement;
import org.apache.iotdb.jdbc.IoTDBSQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class MetricsManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(MetricsManager.class);

    private MetricsManager() {}

    private static void createNewMetric(String name) throws SQLException {
        Statement statement = IoTDBUtil.getConnection().createStatement();
        statement.execute(String.format("CREATE TIMESERIES root.vehicle.%s WITH DATATYPE=INT32, ENCODING=RLE", name));
        statement.close();
    }

    public static void addDatapoint(String name, ImmutableSortedMap<String, String> tags, String type, Long timestamp, String value) throws SQLException {
        String[] names = name.split("\\.");
        PreparedStatement pst = null;
        ResultSet rs = null;
        try {
            pst = IoTDBUtil.getPreparedStatement(String.format("insert into root.vehicle.%s(timestamp,%s) values(%s,%s);", names[0], names[1], timestamp, value), null);
            pst.executeUpdate();
            rs = pst.getResultSet();
        } catch (IoTDBSQLException e) {
            createNewMetric(name);
            addDatapoint(name, tags, type, timestamp, value);
        } catch (SQLException e) {
            throw e;
        } finally {
            if (rs != null)
                try {
                    rs.close();
                } catch (SQLException e) {
                    LOGGER.warn(e.getMessage());
                }
            if (pst != null)
                try {
                    pst.close();
                } catch (SQLException e){
                    LOGGER.warn(e.getMessage());
                }
        }
    }

}
