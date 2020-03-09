package cn.edu.tsinghua.iotdb.kairosdb.tsdb;

import cn.edu.tsinghua.iotdb.kairosdb.http.rest.json.DataPointsParser.DataType;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBWrapper implements IDatabase {
  private static final Logger LOGGER = LoggerFactory.getLogger(DBWrapper.class);
  private IDatabase db;

  public DBWrapper(String dbType, boolean isSession, String url) {
    DBFactory dbFactory = new DBFactory();
    try {
      db = dbFactory.getDatabase(dbType, isSession, url);
    } catch (Exception e) {
      LOGGER.error("Failed to get database because", e);
    }
  }

  @Override
  public void insert(String deviceId, long timestamp, List<String> measurements,
      List<String> values) {
    db.insert(deviceId, timestamp, measurements, values);
  }

  @Override
  public void rangeQuery() {

  }

  @Override
  public void createTimeSeries(Map<String, DataType> seriesPaths) throws SQLException {
    db.createTimeSeries(seriesPaths);
  }
}
