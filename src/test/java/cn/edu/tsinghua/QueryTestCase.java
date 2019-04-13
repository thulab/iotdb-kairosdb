package cn.edu.tsinghua;

import cn.edu.tsinghua.iotdb.kairosdb.datastore.TimeUnit;
import cn.edu.tsinghua.iotdb.kairosdb.http.rest.json.TimeUnitDeserializer;
import cn.edu.tsinghua.iotdb.kairosdb.query.Query;
import cn.edu.tsinghua.iotdb.kairosdb.query.QuerySqlBuilder;
import cn.edu.tsinghua.iotdb.kairosdb.query.result.MetricResult;
import cn.edu.tsinghua.iotdb.kairosdb.query.result.MetricValueResult;
import cn.edu.tsinghua.iotdb.kairosdb.query.result.QueryDataPoint;
import cn.edu.tsinghua.iotdb.kairosdb.query.result.QueryResult;
import cn.edu.tsinghua.iotdb.kairosdb.query.group_by.GroupBy;
import cn.edu.tsinghua.iotdb.kairosdb.query.group_by.GroupByDeserializer;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.util.LinkedList;
import java.util.List;

public class QueryTestCase {

  private static final String jsonStr = "{\n"
      + "  \"start_absolute\": 1357023600000,\n"
      + "  \"end_relative\": {\n"
      + "    \"value\": \"5\",\n"
      + "    \"unit\": \"days\"\n"
      + "  },\n"
      + "  \"time_zone\": \"Asia/Kabul\",\n"
      + "  \"metrics\": [\n"
      + "    {\n"
      + "      \"tags\": {\n"
      + "        \"host\": [\n"
      + "          \"foo\",\n"
      + "          \"foo2\"\n"
      + "        ],\n"
      + "        \"customer\": [\n"
      + "          \"bar\"\n"
      + "        ]\n"
      + "      },\n"
      + "      \"name\": \"abc.123\",\n"
      + "      \"limit\": 10000,\n"
      + "      \"aggregators\": [\n"
      + "        {\n"
      + "          \"name\": \"sum\",\n"
      + "          \"sampling\": {\n"
      + "            \"value\": 10,\n"
      + "            \"unit\": \"minutes\"\n"
      + "          }\n"
      + "        }\n"
      + "      ],\n"
      + "      \"group_by\":[\n"
      + "        {\n"
      + "          \"name\": \"time\",\n"
      + "          \"group_count\": \"4\",\n"
      + "          \"range_size\": {\n"
      + "            \"value\": \"1\",\n"
      + "            \"unit\": \"milliseconds\"\n"
      + "          }\n"
      + "        }\n"
      + "      ]\n"
      + "    },\n"
      + "    {\n"
      + "      \"tags\": {\n"
      + "        \"host\": [\n"
      + "          \"foo\",\n"
      + "          \"foo2\"\n"
      + "        ],\n"
      + "        \"customer\": [\n"
      + "          \"bar\"\n"
      + "        ]\n"
      + "      },\n"
      + "      \"name\": \"xyz.123\",\n"
      + "      \"aggregators\": [\n"
      + "        {\n"
      + "          \"name\": \"avg\",\n"
      + "          \"sampling\": {\n"
      + "            \"value\": 10,\n"
      + "            \"unit\": \"minutes\"\n"
      + "          }\n"
      + "        }\n"
      + "      ]\n"
      + "    }\n"
      + "  ]\n"
      + "}";

  private static void queryJson() {
    Gson gson = new GsonBuilder()
        .registerTypeAdapter(TimeUnit.class, new TimeUnitDeserializer())
        .registerTypeAdapter(GroupBy.class, new GroupByDeserializer())
        .disableHtmlEscaping()
        .create();

    Query query;
    query = gson.fromJson(jsonStr, Query.class);
  }

  private static void queryResult() {

    MetricValueResult metricValueResult = new MetricValueResult("abc123");
    metricValueResult.addTag("host", "123");
    metricValueResult.addTag("host", "456");
    metricValueResult.addTag("db", "dc1");
    metricValueResult.addDataPoint(new QueryDataPoint(1234455L, 12.3));
    metricValueResult.addDataPoint(new QueryDataPoint(1234456L, 12.6));

    MetricResult metricResult = new MetricResult();
    metricResult.addResult(metricValueResult);


    QueryResult queryResult = new QueryResult();
//    queryResult.addMetricResult(metricResult);
    queryResult.addVoidMetricResult("abc123");

    Gson gson = new GsonBuilder()
        .registerTypeAdapter(QueryDataPoint.class, new QueryDataPoint())
        .disableHtmlEscaping()
        .create();

    String json = gson.toJson(queryResult);

    int i = 0;
  }

  private static void querySqlBuilder() {
    QuerySqlBuilder builder = new QuerySqlBuilder("group_1");
    List<String> list_1 = new LinkedList<>();
    list_1.add("host1");
    list_1.add("host2");
    List<String> list_2 = new LinkedList<>();
    list_2.add("DC1");
    list_2.add("DC2");
    builder.append(list_1);
    builder.append(list_2);
    builder.append("123");

    String sql = builder.generateSql();

    int i = 1;
  }

  public static void main(String[] argv) {
    querySqlBuilder();
  }

}
