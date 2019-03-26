package cn.edu.tsinghua;

import cn.edu.tsinghua.iotdb.kairosdb.dao.IoTDBUtil;
import cn.edu.tsinghua.iotdb.kairosdb.dao.MetricsManager;
import com.google.common.collect.ImmutableSortedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class MetricsManagerTestCase {

    private static final Logger LOGGER = LoggerFactory.getLogger(MetricsManagerTestCase.class);

    private static void initDB() throws SQLException, ClassNotFoundException {
        IoTDBUtil.initConnection("127.0.0.1", "6667", "root", "root");
        MetricsManager.loadMetadata();
    }

    public static void main(String[] argv) {
        Map<String, String > tags = new HashMap<>();
        tags.put("host", "server1");
        tags.put("data-center", "DC1");

        String name = "arc";
        Long timestamp = 123L;
        String value = "abcd";

        try {
            initDB();
            MetricsManager.addDatapoint(name, ImmutableSortedMap.copyOf(tags), "string", timestamp, value);
        } catch (Exception e) {
            LOGGER.error(String.format("%s: %s", e.getClass().getName(), e.getMessage()));
        }

    }

}
