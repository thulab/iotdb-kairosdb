package cn.edu.tsinghua.iotdb.kairosdb.tsdb.iotdb;

import cn.edu.tsinghua.iotdb.kairosdb.http.rest.json.DataPointsParser.DataType;
import cn.edu.tsinghua.iotdb.kairosdb.tsdb.IDatabase;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.session.IoTDBSessionException;
import org.apache.iotdb.session.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IoTDBSession implements IDatabase {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBSession.class);
  private Session session;

  public IoTDBSession(String url) {
    String host = url.split(":")[0];
    int port = Integer.parseInt(url.split(":")[1]);
    session = new Session(host, port, "root", "root");
    try {
      session.open();
    } catch (IoTDBSessionException e) {
      LOGGER.error("Create IoTDB session failed ", e);
    }
  }


  @Override
  public void insert(String deviceId, long timestamp, List<String> measurements,
      List<String> values) {
    try {
      session.insert(deviceId, timestamp, measurements, values);
    } catch (IoTDBSessionException e) {
      LOGGER.error("Insert data of device {} failed, timestamp = {}", deviceId, timestamp, e);
    }
  }

  @Override
  public void rangeQuery() {

  }

  @Override
  public void createTimeSeries(Map<String, DataType> seriesPaths) {

  }
}
