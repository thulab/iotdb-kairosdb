package cn.edu.tsinghua.iotdb.kairosdb.tsdb;

import cn.edu.tsinghua.iotdb.kairosdb.conf.Constants;
import cn.edu.tsinghua.iotdb.kairosdb.tsdb.influxdb.Influx;
import cn.edu.tsinghua.iotdb.kairosdb.tsdb.iotdb.IoTDB;
import cn.edu.tsinghua.iotdb.kairosdb.tsdb.iotdb.IoTDBSession;
import java.sql.SQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(DBFactory.class);

  public IDatabase getDatabase(String dbType, boolean isSession, String url) throws SQLException {

    switch (dbType) {
      case Constants.DB_IOT:
        if(isSession) {
          return new IoTDBSession(url);
        } else {
          return new IoTDB(url);
        }
      case Constants.DB_INFLUX:
        return new Influx(url);
      default:
        LOGGER.error("unsupported database {}", dbType);
        throw new SQLException("unsupported database " + dbType);
    }
  }

}
