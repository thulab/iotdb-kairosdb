package cn.edu.tsinghua.iotdb.kairosdb.query.sql_builder;

import cn.edu.tsinghua.iotdb.kairosdb.dao.MetricsManager;
import cn.edu.tsinghua.iotdb.kairosdb.query.QueryExecutor;
import java.util.LinkedList;
import java.util.List;

public class QuerySqlBuilder {

  private static final String SQL_PREFIX = "SELECT %s FROM ";

  private String metricName;

  private List<StringBuilder> list;

  public QuerySqlBuilder(String metricName) {
    this.metricName = metricName;
    list = new LinkedList<>();
    list.add(new StringBuilder("root.*"));
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

  public String generateSql(long startTime, long endTime) {
    return String.format("%s where time>=%s and time<=%s", toString(), startTime, endTime);
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append(String.format(SQL_PREFIX, metricName));
    for (StringBuilder tmpBuilder : list) {
      builder.append(tmpBuilder);
      builder.append(",");
    }
    builder.deleteCharAt(builder.length() - 1);
    return builder.toString();
  }
}
