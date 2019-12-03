package cn.edu.tsinghua;

import cn.edu.tsinghua.iotdb.kairosdb.dao.IoTDBUtil;
import cn.edu.tsinghua.iotdb.kairosdb.dao.MetricsManager;
import cn.edu.tsinghua.iotdb.kairosdb.http.rest.json.DataPointsParser;
import cn.edu.tsinghua.iotdb.kairosdb.http.rest.json.DataPointsParser.DataType;
import com.google.common.collect.ImmutableSortedMap;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsManagerTestCase {

  private static final Logger LOGGER = LoggerFactory.getLogger(MetricsManagerTestCase.class);

  private static void initDB() throws SQLException, ClassNotFoundException {
    Connection connection = IoTDBUtil.getConnection("127.0.0.1:6667", "root", "root");
    MetricsManager.loadMetadata(connection);
  }

  public static void main(String[] argv) {
    Map<String, String> tags = new HashMap<>();
    tags.put("host", "server1");
    tags.put("data-center", "DC1");

    String name = "arc";
    Long timestamp = 123L;
    String value = "abcd";

    try {
      initDB();
      DataPointsParser dpp = new DataPointsParser(null, null);
      dpp.addDataPoint(name, ImmutableSortedMap.copyOf(tags), DataType.STRING, timestamp, value);
    } catch (Exception e) {
      LOGGER.error(String.format("%s: %s", e.getClass().getName(), e.getMessage()));
    }

  }

}
