package cn.edu.tsinghua.iotdb.kairosdb.http.rest.json;

import cn.edu.tsinghua.iotdb.kairosdb.conf.Config;
import cn.edu.tsinghua.iotdb.kairosdb.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.kairosdb.dao.IoTDBConnectionPool;
import cn.edu.tsinghua.iotdb.kairosdb.dao.MetricsManager;
import cn.edu.tsinghua.iotdb.kairosdb.util.Util;
import cn.edu.tsinghua.iotdb.kairosdb.util.ValidationException;
import cn.edu.tsinghua.iotdb.kairosdb.util.Validator;
import com.google.common.collect.ImmutableSortedMap;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSyntaxException;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import java.io.EOFException;
import java.io.IOException;
import java.io.Reader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DataPointsParser {

  private static final Logger LOGGER = LoggerFactory.getLogger(DataPointsParser.class);
  private static final Config config = ConfigDescriptor.getInstance().getConfig();
  private static AtomicLong loop = new AtomicLong(0);
  private static AtomicLong parseJsonAccumulatedTime = new AtomicLong(0);
  private static AtomicLong jdbcAccumulatedTime = new AtomicLong(0);
  private static AtomicLong parseTotalAccumulatedTime = new AtomicLong(0);
  private static final int OUTPUT_THRESHOLD = 5000;
  private final Reader inputStream;
  private final Gson gson;

  // <timestamp-path, <metric, value>>
  private Map<TimestampDevicePair, Map<String, String>> tableMap = new HashMap<>();
  // <path, type>
  private Map<String, DataType> seriesPaths = new HashMap<>();

  // The constants of encoding methods
  private static final String TEXT_ENCODING = "PLAIN";
  private static final String INT64_ENCODING = "TS_2DIFF";
  private static final String DOUBLE_ENCODING = "GORILLA";
  private List<Connection> connections = new ArrayList<>();

  public DataPointsParser(Reader stream, Gson gson) {
    connections = IoTDBConnectionPool.getInstance().getConnections();
    this.inputStream = stream;
    this.gson = gson;
  }

  public ValidationErrors parse() throws IOException {
    long start = 0;
    long id = System.currentTimeMillis();
    long prepareSqlTime = 0;
    long jdbcTime = 0;

    if (config.DEBUG == 1) {
      start = System.currentTimeMillis();
    }
    ValidationErrors validationErrors = new ValidationErrors();
    try (JsonReader reader = new JsonReader(inputStream)) {
      int metricCount = 0;
      if (reader.peek().equals(JsonToken.BEGIN_ARRAY)) {
        try {
          reader.beginArray();

          while (reader.hasNext()) {

            NewMetric metric = parseMetric(reader);
            validateAndAddDataPoints(metric, validationErrors, metricCount);
            metricCount++;
          }
        } catch (EOFException e) {
          validationErrors.addErrorMessage("Invalid json. No content due to end of input.");
        }

        reader.endArray();
      } else if (reader.peek().equals(JsonToken.BEGIN_OBJECT)) {
        NewMetric metric = parseMetric(reader);
        validateAndAddDataPoints(metric, validationErrors, 0);
      } else {
        validationErrors.addErrorMessage("Invalid start of json.");
      }

    } catch (EOFException e) {
      validationErrors.addErrorMessage("Invalid json. No content due to end of input.");
    }
    if (config.DEBUG == 1) {
      prepareSqlTime = System.currentTimeMillis() - start;
    }

    if (config.DEBUG == 1) {
      start = System.currentTimeMillis();
    }
    try {
      sendMetricsData();
    } catch (SQLException e) {
      try {
        createTimeSeries();
        sendMetricsData();
      } catch (SQLException ex) {
        try {
          sendMetricsData();
        } catch (SQLException exc) {
          LOGGER.debug("Exception occur:", exc);
        }
        LOGGER.debug("Exception occur:", ex);
        validationErrors.addErrorMessage(
            String.format("%s: %s", ex.getClass().getName(), ex.getMessage()));
      }
    }

    if (config.DEBUG == 1) {
      jdbcTime = System.currentTimeMillis() - start;
      long parseTotalTime = System.currentTimeMillis() - id;
      long currLoop = loop.incrementAndGet();
      long parse = parseJsonAccumulatedTime.addAndGet(prepareSqlTime);
      long jdbc = jdbcAccumulatedTime.addAndGet(jdbcTime);
      long total = parseTotalAccumulatedTime.addAndGet(parseTotalTime);
      if (currLoop % OUTPUT_THRESHOLD == 0) {
        LOGGER.info(
            "loop ,{}, parse()中解析JSON及准备SQL平均耗时: ,{}, ms IoTDB JDBC平均耗时: ,{}, ms, 总平均耗时 ,{}, ms",
            currLoop, (parse * 1.0F / currLoop), (jdbc * 1.0F / currLoop),
            (total * 1.0F / currLoop));
      }
    }

    return validationErrors;
  }

  private static String createTimeSeriesSql(String seriesPath, DataType type) {
    String datatype;
    String encoding;
    switch (type) {
      case LONG:
        datatype = "INT64";
        encoding = INT64_ENCODING;
        break;
      case DOUBLE:
        datatype = "DOUBLE";
        encoding = DOUBLE_ENCODING;
        break;
      default:
        datatype = "TEXT";
        encoding = TEXT_ENCODING;
    }
    return String
        .format("CREATE TIMESERIES %s WITH DATATYPE=%s, ENCODING=%s, COMPRESSOR=SNAPPY", seriesPath,
            datatype, encoding);
  }

  private void createTimeSeries() throws SQLException {
    for (Connection conn : connections) {
      try (Statement statement = conn.createStatement()) {
        for (Map.Entry<String, DataType> entry : seriesPaths.entrySet()) {
          statement.addBatch(createTimeSeriesSql(entry.getKey(), entry.getValue()));
        }
        statement.executeBatch();
      }
    }
  }

  private void sendMetricsData() throws SQLException {
    long start = 0;
    if (config.DEBUG == 2) {
      start = System.currentTimeMillis();
    }
    for (Connection conn : connections) {
      try (Statement statement = conn.createStatement()) {
        for (Map.Entry<TimestampDevicePair, Map<String, String>> entry : tableMap.entrySet()) {
          StringBuilder sqlBuilder = new StringBuilder();
          StringBuilder sensorPartBuilder = new StringBuilder("(timestamp");
          StringBuilder valuePartBuilder = new StringBuilder(" values(");
          long timestamp = entry.getKey().getTimestamp();
          String path = entry.getKey().getDevice();
          String sqlPrefix = String
              .format("insert into root.%s%s", MetricsManager.getStorageGroupName(path), path);
          valuePartBuilder.append(timestamp);
          for (Map.Entry<String, String> subEntry : entry.getValue().entrySet()) {
            sensorPartBuilder.append(",").append(subEntry.getKey());
            valuePartBuilder.append(",").append(subEntry.getValue());
          }
          sensorPartBuilder.append(")");
          valuePartBuilder.append(")");
          sqlBuilder.append(sqlPrefix).append(sensorPartBuilder).append(valuePartBuilder);
          statement.execute(sqlBuilder.toString());
        }
      }
    }
    if (config.DEBUG == 2) {
      long elapse = System.currentTimeMillis() - start;
      LOGGER.info("sendMetricsData() 执行的时间: ,{}, ms", elapse);
    }
  }

  private NewMetric parseMetric(JsonReader reader) {
    NewMetric metric;
    try {
      metric = gson.fromJson(reader, NewMetric.class);
    } catch (IllegalArgumentException e) {
      // Happens when parsing data points where one of the pair is missing (timestamp or value)
      throw new JsonSyntaxException("Invalid JSON");
    }
    return metric;
  }

  /**
   * Add a new datapoint to database, and automatically create corresponding TIMESERIES to store
   * it.
   *
   * @param name The name of the metric
   * @param tags The tags of the datapoint(at least one)
   * @param type The type of the datapoint value(int, double, text)
   * @param timestamp The timestamp of the datapoint
   * @param value The value of the datapoint
   * @return Null if the datapoint has been correctly insert, otherwise, the errors in
   * ValidationErrors
   */
  public ValidationErrors addDataPoint(String name, ImmutableSortedMap<String, String> tags,
      DataType type, Long timestamp, String value) throws SQLException {
    ValidationErrors validationErrors = new ValidationErrors();
    if (null == tags) {
      LOGGER.error("metric {} have no tag", name);
      validationErrors.addErrorMessage(String.format("metric %s have no tag", name));
      return validationErrors;
    }

    HashMap<Integer, String> orderTagKeyMap = MetricsManager.getMapping(name, tags);

    if (type.equals(DataType.STRING)) {
      value = String.format("\"%s\"", value);
    }

    // Generate the path
    String path = MetricsManager.generatePath(tags, orderTagKeyMap);

    seriesPaths
        .put(String.format("root.%s%s.%s", MetricsManager.getStorageGroupName(path), path, name),
            type);
    TimestampDevicePair tableMapKey = new TimestampDevicePair(timestamp, path);
    if (tableMap.containsKey(tableMapKey)) {
      tableMap.get(tableMapKey).put(name, value);
    } else {
      Map<String, String> metricValueMap = new HashMap<>();
      metricValueMap.put(name, value);
      tableMap.put(tableMapKey, metricValueMap);
    }

    return validationErrors;
  }

  private boolean validateAndAddDataPoints(NewMetric metric, ValidationErrors errors, int count) {
    ValidationErrors validationErrors = new ValidationErrors();
    Context context = new Context(count);

    if (Validator
        .isNotNullOrEmpty(validationErrors, context.setAttribute("name"), metric.getName())) {
      context.setName(metric.getName());
    }

    if (metric.getTimestamp() != null) {
      Validator
          .isNotNullOrEmpty(validationErrors, context.setAttribute("value"), metric.getValue());
    } else if (metric.getValue() != null && !metric.getValue().isJsonNull()) {
      Validator
          .isNotNull(validationErrors, context.setAttribute("timestamp"), metric.getTimestamp());
    }

    if (Validator
        .isNotNull(validationErrors, context.setAttribute("tags count"), metric.getTags())) {
      if (Validator.isGreaterThanOrEqualTo(validationErrors, context.setAttribute("tags count"),
          metric.getTags().size(), 1)) {
        int tagCount = 0;
        SubContext tagContext = new SubContext(context.setAttribute(null), "tag");

        for (Map.Entry<String, String> entry : metric.getTags().entrySet()) {
          tagContext.setCount(tagCount);
          if (Validator.isNotNullOrEmpty(validationErrors, tagContext.setAttribute("name"),
              entry.getKey())) {
            tagContext.setName(entry.getKey());
            Validator.isNotNullOrEmpty(validationErrors, tagContext, entry.getKey());
          }
          if (Validator.isNotNullOrEmpty(validationErrors, tagContext.setAttribute("value"),
              entry.getValue())) {
            Validator.isNotNullOrEmpty(validationErrors, tagContext, entry.getValue());
          }

          tagCount++;
        }
      }
    }

    if (!validationErrors.hasErrors()) {
      ImmutableSortedMap<String, String> tags = ImmutableSortedMap.copyOf(metric.getTags());

      if (metric.getTimestamp() != null && metric.getValue() != null) {
        DataType type = null;
        try {
          type = findType(metric.getValue());
        } catch (ValidationException e) {
          validationErrors.addErrorMessage(context + " " + e.getMessage());
        }

        try {
          ValidationErrors tErrors = addDataPoint(metric.getName(), tags, type,
              metric.getTimestamp(),
              metric.getValue().getAsString());
          if (null != tErrors) {
            validationErrors.add(tErrors);
          }
        } catch (SQLException e) {
          validationErrors.addErrorMessage(context + " " + e.getMessage());
        }
      }

      if (metric.getDatapoints() != null && metric.getDatapoints().length > 0) {
        int contextCount = 0;
        SubContext dataPointContext = new SubContext(context, "datapoints");
        for (JsonElement[] dataPoint : metric.getDatapoints()) {
          dataPointContext.setCount(contextCount);
          if (dataPoint.length < 1) {
            validationErrors.addErrorMessage(
                dataPointContext.setAttribute("timestamp") + " cannot be null or empty.");
            continue;
          } else if (dataPoint.length < 2) {
            validationErrors.addErrorMessage(
                dataPointContext.setAttribute("value") + " cannot be null or empty.");
            continue;
          } else {
            Long timestamp = null;
            if (!dataPoint[0].isJsonNull()) {
              timestamp = dataPoint[0].getAsLong();
            }

            if (!Validator
                .isNotNull(validationErrors, dataPointContext.setAttribute("timestamp"),
                    timestamp)) {
              continue;
            }

            DataType type = null;
            if (dataPoint.length > 2) {
              type = toEnumType(dataPoint[2].getAsString());
            }

            if (!Validator
                .isNotNullOrEmpty(validationErrors, dataPointContext.setAttribute("value"),
                    dataPoint[1])) {
              continue;
            }

            if (type == null) {
              try {
                type = findType(dataPoint[1]);
              } catch (ValidationException e) {
                validationErrors.addErrorMessage(context + " " + e.getMessage());
                continue;
              }
            }

            try {
              ValidationErrors tErrors = addDataPoint(metric.getName(), tags, type, timestamp,
                  dataPoint[1].getAsString());
              if (null != tErrors) {
                validationErrors.add(tErrors);
              }
            } catch (SQLException e) {
              validationErrors.addErrorMessage(context + " " + e.getMessage());
            }

          }
          contextCount++;
        }
      }
    }

    errors.add(validationErrors);

    return !validationErrors.hasErrors();
  }

  private DataType toEnumType(String s) {
    switch (s) {
      case "string":
        return DataType.STRING;
      case "double":
        return DataType.DOUBLE;
      case "long":
        return DataType.LONG;
      default:
        return null;
    }
  }

  private DataType findType(JsonElement value) throws ValidationException {
    if (!value.isJsonPrimitive()) {
      throw new ValidationException("value is an invalid type");
    }

    JsonPrimitive primitiveValue = (JsonPrimitive) value;
    if (primitiveValue.isNumber() || (primitiveValue.isString() && Util
        .isNumber(value.getAsString()))) {
      String v = value.getAsString();

      if (!v.contains(".")) {
        return DataType.LONG;
      } else {
        return DataType.DOUBLE;
      }
    } else {
      return DataType.STRING;
    }
  }

  public enum DataType {
    LONG,
    DOUBLE,
    STRING,
  }

  private static class Context {

    private int m_count;
    private String m_name;
    private String m_attribute;

    Context(int count) {
      m_count = count;
    }

    private Context setName(String name) {
      m_name = name;
      m_attribute = null;
      return (this);
    }

    private Context setAttribute(String attribute) {
      m_attribute = attribute;
      return (this);
    }

    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("metric[").append(m_count).append("]");
      if (m_name != null) {
        sb.append("(name=").append(m_name).append(")");
      }

      if (m_attribute != null) {
        sb.append(".").append(m_attribute);
      }

      return (sb.toString());
    }
  }

  private static class SubContext {

    private Context m_context;
    private String m_contextName;
    private int m_count;
    private String m_name;
    private String m_attribute;

    SubContext(Context context, String contextName) {
      m_context = context;
      m_contextName = contextName;
    }

    private SubContext setCount(int count) {
      m_count = count;
      m_name = null;
      m_attribute = null;
      return (this);
    }

    private SubContext setName(String name) {
      m_name = name;
      m_attribute = null;
      return (this);
    }

    private SubContext setAttribute(String attribute) {
      m_attribute = attribute;
      return (this);
    }

    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(m_context).append(".").append(m_contextName).append("[");
      if (m_name != null) {
        sb.append(m_name);
      } else {
        sb.append(m_count);
      }
      sb.append("]");

      if (m_attribute != null) {
        sb.append(".").append(m_attribute);
      }

      return (sb.toString());
    }
  }

  private static class NewMetric {

    private String name;
    private Long timestamp = null;
    private Long time = null;
    private JsonElement value;
    private Map<String, String> tags;
    private JsonElement[][] datapoints;
    private int ttl = 0;

    public String getName() {
      return name;
    }

    public Long getTimestamp() {
      if (time != null) {
        return time;
      } else {
        return timestamp;
      }
    }

    public JsonElement getValue() {
      return value;
    }

    public Map<String, String> getTags() {
      return tags;
    }

    JsonElement[][] getDatapoints() {
      return datapoints;
    }

    public int getTtl() {
      return ttl;
    }
  }

}
