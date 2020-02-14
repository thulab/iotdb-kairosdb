package cn.edu.tsinghua.iotdb.kairosdb.dao;

import java.util.HashMap;
import java.util.Map;

public class SchemaConvertUtil {

  public static String fromMetricToFullPath(String metricName, Map<String, String> tags) {
    HashMap<Integer, String> orderTagKeyMap = MetricsManager.getMapping(metricName, tags);
    // Generate the path
    String path = MetricsManager.generatePath(tags, orderTagKeyMap);
    return String.format("root.%s%s.%s", MetricsManager.getStorageGroupName(path), path,
        metricName);
  }

  public static String fromMetricToDeviceId(String metricName, Map<String, String> tags) {
    HashMap<Integer, String> orderTagKeyMap = MetricsManager.getMapping(metricName, tags);
    String path = MetricsManager.generatePath(tags, orderTagKeyMap);
    return String.format("root.%s%s", MetricsManager.getStorageGroupName(path), path);
  }
}
