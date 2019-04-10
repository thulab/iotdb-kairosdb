package cn.edu.tsinghua.iotdb.kairosdb.http.rest.json;

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
import java.sql.SQLException;
import java.util.Map;


public class DataPointsParser {

  private final Reader inputStream;
  private final Gson gson;

  private int ingestTime;
  private int dataPointCount;

  public DataPointsParser(Reader stream, Gson gson) {
    this.inputStream = stream;
    this.gson = gson;
  }

  public int getIngestTime() {
    return ingestTime;
  }

  public int getDataPointCount() {
    return dataPointCount;
  }

  public ValidationErrors parse() throws IOException {

    long start = System.currentTimeMillis();
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

    ingestTime = (int) (System.currentTimeMillis() - start);

    return validationErrors;
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
        String type = null;
        try {
          type = findType(metric.getValue());
        } catch (ValidationException e) {
          validationErrors.addErrorMessage(context + " " + e.getMessage());
        }

        try {
          ValidationErrors tErrors = MetricsManager.addDatapoint(metric.getName(), tags, type, metric.getTimestamp(),
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

            String type = null;
            if (dataPoint.length > 2) {
              type = dataPoint[2].getAsString();
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
              ValidationErrors tErrors = MetricsManager.addDatapoint(metric.getName(), tags, type, timestamp,
                  dataPoint[1].getAsString());
              if (null != tErrors) {
                validationErrors.add(tErrors);
              }
            } catch (SQLException e) {
              validationErrors.addErrorMessage(context + " " + e.getMessage());
            }

            dataPointCount++;
          }
          contextCount++;
        }
      }
    }

    errors.add(validationErrors);

    return !validationErrors.hasErrors();
  }

  private String findType(JsonElement value) throws ValidationException {
    if (!value.isJsonPrimitive()) {
      throw new ValidationException("value is an invalid type");
    }

    JsonPrimitive primitiveValue = (JsonPrimitive) value;
    if (primitiveValue.isNumber() || (primitiveValue.isString() && Util
        .isNumber(value.getAsString()))) {
      String v = value.getAsString();

      if (!v.contains(".")) {
        return "long";
      } else {
        return "double";
      }
    } else {
      return "string";
    }
  }

  private static class Context {

    private int m_count;
    private String m_name;
    private String m_attribute;

    public Context(int count) {
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

    public SubContext(Context context, String contextName) {
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

    public JsonElement[][] getDatapoints() {
      return datapoints;
    }

    public int getTtl() {
      return ttl;
    }
  }

}
