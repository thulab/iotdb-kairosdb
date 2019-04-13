package cn.edu.tsinghua;

import cn.edu.tsinghua.iotdb.kairosdb.datastore.TimeUnit;
import cn.edu.tsinghua.iotdb.kairosdb.http.rest.json.TimeUnitDeserializer;
import cn.edu.tsinghua.iotdb.kairosdb.query.Query;
import cn.edu.tsinghua.iotdb.kairosdb.query.group_by.GroupBy;
import cn.edu.tsinghua.iotdb.kairosdb.query.group_by.GroupByDeserializer;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryParserTestCase {

  private static final Logger LOGGER = LoggerFactory.getLogger(QueryParserTestCase.class);

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

  public static void main(String[] argv) {
    GsonBuilder builder = new GsonBuilder();
    Gson gson = builder
        .registerTypeAdapter(GroupBy.class, new GroupByDeserializer())
        .registerTypeAdapter(TimeUnit.class, new TimeUnitDeserializer())
        .disableHtmlEscaping()
        .create();

    Query query;
    query = gson.fromJson(jsonStr, Query.class);

    int i=0;
  }

}
