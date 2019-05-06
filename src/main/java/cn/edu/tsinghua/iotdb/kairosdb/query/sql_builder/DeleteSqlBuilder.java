package cn.edu.tsinghua.iotdb.kairosdb.query.sql_builder;

import cn.edu.tsinghua.iotdb.kairosdb.query.QueryExecutor;
import java.sql.Types;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class DeleteSqlBuilder {

  public static String NULL_STR = "2147483646";

  private Map<String, List<String>> map;

  public DeleteSqlBuilder() {
    map = new HashMap<>();
  }

  public void appendDataPoint(String path, String timestamp) {
    List<String> timestamps = map.getOrDefault(path, null);
    if (timestamps == null) {
      timestamps = new LinkedList<>();
      map.put(path, timestamps);
    }
    timestamps.add(timestamp);
  }

  public List<String> build(String[] paths, int[] types) {
    Map<String, Integer> mapping = new HashMap<>();
    int size = paths.length;
    for (int i = 0; i < size; i++) {
      mapping.put(paths[i], types[i]);
    }
    return build(mapping);
  }

  private List<String> build(Map<String, Integer> types) {
    List<String> list = new LinkedList<>();

    for (Entry<String, List<String>> entry : map.entrySet()) {
      StringBuilder builder = new StringBuilder("insert into ");
      int index = entry.getKey().lastIndexOf('.');
      builder.append(entry.getKey().substring(0, index));
      builder.append("(timestamp, ");
      builder.append(entry.getKey().substring(index + 1));
      builder.append(") values(");

      String value = "";
      switch (types.getOrDefault(entry.getKey(), -1)) {
        case Types.INTEGER: case Types.DOUBLE:
          value = NULL_STR;
          break;
        case Types.VARCHAR:
          value = String.format("\"%s\"", NULL_STR);
          break;
        default:
          QueryExecutor.LOGGER.error("Data Type Error!");
          continue;
      }

      for (String timestamp : entry.getValue()) {
        list.add(String.format("%s%s, %s);", builder.toString(), timestamp, value));
      }

    }

    return list;
  }


}
