package cn.edu.tsinghua.iotdb.kairosdb.query;

import cn.edu.tsinghua.iotdb.kairosdb.dao.MetricsManager;
import java.util.LinkedList;
import java.util.List;

public class QuerySqlBuilder {

  private static final String sqlPrefix = "SELECT %s FROM ";

  private String metricName;

  private List<StringBuilder> list;

  public QuerySqlBuilder(String metricName) {
    this.metricName = metricName;
    list = new LinkedList<>();
    list.add(new StringBuilder(String.format("root.%s", MetricsManager.getStorageGroupName(metricName))));
  }

  public QuerySqlBuilder append(String path) {
    if (path == null) {
      QueryExecutor.LOGGER.error("Among QuerySqlBuilder.append(String path), path could not be null.");
    } else{
      for (StringBuilder builder : list) {
        builder.append(".");
        builder.append(path);
      }
    }
    return this;
  }

  public QuerySqlBuilder append(List<String> paths) {
    List<StringBuilder> cacheList = new LinkedList<>();

    for (StringBuilder builder : list) {
      for (String path : paths) {
        StringBuilder tmpBuilder = new StringBuilder(builder);
        tmpBuilder.append(".");
        tmpBuilder.append(path);
        cacheList.add(tmpBuilder);
      }
    }

    list = cacheList;

    return this;
  }

  public String generateSql() {
    return toString();
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append(String.format(sqlPrefix, metricName));
    for (StringBuilder tmpBuilder : list) {
      builder.append(tmpBuilder);
      builder.append(",");
    }
    builder.deleteCharAt(builder.length() - 1);
    return builder.toString();
  }
}
