package cn.edu.tsinghua.iotdb.kairosdb.tsdb;


import cn.edu.tsinghua.iotdb.kairosdb.http.rest.json.DataPointsParser.DataType;
import cn.edu.tsinghua.iotdb.kairosdb.query.QueryMetric;
import cn.edu.tsinghua.iotdb.kairosdb.query.result.MetricValueResult;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public interface IDatabase {

  void insert(String deviceId, long timestamp, List<String> measurements, List<String> values);

  void rangeQuery(String sql, long metricCount, AtomicLong sampleSize,
      MetricValueResult metricValueResult, AtomicBoolean hasMetaData, QueryMetric metric);

  void createTimeSeries(Map<String, DataType> seriesPaths) throws SQLException;
  
  void executeSQL(String sql) throws SQLException;

  void addSaveFromData(MetricValueResult valueResult, String path, String metricName)
      throws SQLException;

  void deleteMetric(
      Map<String, Map<String, Integer>> tagOrder, String metricName) throws SQLException;

  void delete(String querySql);

  long getValueResult(String sql, MetricValueResult metricValueResult);
}
