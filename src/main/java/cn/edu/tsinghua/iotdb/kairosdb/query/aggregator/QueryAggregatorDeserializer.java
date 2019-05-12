package cn.edu.tsinghua.iotdb.kairosdb.query.aggregator;

import cn.edu.tsinghua.iotdb.kairosdb.datastore.Duration;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import java.lang.reflect.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryAggregatorDeserializer implements JsonDeserializer<QueryAggregator> {

  private static final Logger LOGGER = LoggerFactory.getLogger(QueryAggregatorDeserializer.class);

  private static final String SAMPLING = "sampling";

  @Override
  public QueryAggregator deserialize(JsonElement jsonElement, Type type,
      JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {

    String name = getAggregatorName(jsonElement);
    QueryAggregator aggregator;
    Duration duration;
    try {
      QueryAggregatorType queryAggregatorType = QueryAggregatorType.fromString(name);
      switch (queryAggregatorType) {
        case AVG:
          QueryAggregatorAvg avgAggregator = new QueryAggregatorAvg();
          duration = jsonDeserializationContext.deserialize(
              jsonElement.getAsJsonObject().get(SAMPLING), Duration.class);
          avgAggregator.setSampling(duration);
          avgAggregator.setAlign(getAlign(jsonElement));
          aggregator = avgAggregator;
          break;
        case DEV:
          QueryAggregatorDev devAggregator = new QueryAggregatorDev();
          duration = jsonDeserializationContext.deserialize(
              jsonElement.getAsJsonObject().get(SAMPLING), Duration.class);
          devAggregator.setSampling(duration);
          devAggregator.setAlign(getAlign(jsonElement));
          JsonElement returnTypeEle = jsonElement.getAsJsonObject().get("return_type");
          if (returnTypeEle == null) {
            throw new JsonParseException("Among aggregator dev, [return_type] must be specified");
          }
          if (!devAggregator.setReturnTypeFromString(returnTypeEle.getAsString())) {
            throw new JsonParseException("Among aggregator dev, [return_type] must be one of "
                + "[\"value\", \"pos_sd\", \"neg_sd\"]");
          }
          aggregator = devAggregator;
          break;
        case COUNT:
          QueryAggregatorCount countAggregator = new QueryAggregatorCount();
          duration = jsonDeserializationContext.deserialize(
              jsonElement.getAsJsonObject().get(SAMPLING), Duration.class);
          countAggregator.setSampling(duration);
          countAggregator.setAlign(getAlign(jsonElement));
          aggregator = countAggregator;
          break;
        case FIRST:
          QueryAggregatorFirst firstAggregator = new QueryAggregatorFirst();
          duration = jsonDeserializationContext.deserialize(
              jsonElement.getAsJsonObject().get(SAMPLING), Duration.class);
          firstAggregator.setSampling(duration);
          firstAggregator.setAlign(getAlign(jsonElement));
          aggregator = firstAggregator;
          break;
        case LAST:
          QueryAggregatorLast lastAggregator = new QueryAggregatorLast();
          duration = jsonDeserializationContext.deserialize(
              jsonElement.getAsJsonObject().get(SAMPLING), Duration.class);
          lastAggregator.setSampling(duration);
          lastAggregator.setAlign(getAlign(jsonElement));
          aggregator = lastAggregator;
          break;
        case MAX:
          QueryAggregatorMax maxAggregator = new QueryAggregatorMax();
          duration = jsonDeserializationContext.deserialize(
              jsonElement.getAsJsonObject().get(SAMPLING), Duration.class);
          maxAggregator.setSampling(duration);
          maxAggregator.setAlign(getAlign(jsonElement));
          aggregator = maxAggregator;
          break;
        case MIN:
          QueryAggregatorMin minAggregator = new QueryAggregatorMin();
          duration = jsonDeserializationContext.deserialize(
              jsonElement.getAsJsonObject().get(SAMPLING), Duration.class);
          minAggregator.setSampling(duration);
          minAggregator.setAlign(getAlign(jsonElement));
          aggregator = minAggregator;
          break;
        case PERCENTILE:
          QueryAggregatorPercentile percentileAggregator = new QueryAggregatorPercentile();
          duration = jsonDeserializationContext.deserialize(
              jsonElement.getAsJsonObject().get(SAMPLING), Duration.class);
          percentileAggregator.setSampling(duration);
          JsonElement percentileEle = jsonElement.getAsJsonObject().get("percentile");
          if (percentileEle == null) {
            throw new JsonParseException("Among aggregator percentile, [percentile] must be specified");
          }
          percentileAggregator.setPercentile(percentileEle.getAsDouble());
          aggregator = percentileAggregator;
          break;
        case SUM:
          QueryAggregatorSum sumAggregator = new QueryAggregatorSum();
          duration = jsonDeserializationContext.deserialize(
              jsonElement.getAsJsonObject().get(SAMPLING), Duration.class);
          sumAggregator.setSampling(duration);
          sumAggregator.setAlign(getAlign(jsonElement));
          aggregator = sumAggregator;
          break;
        case DIFF:
          aggregator = new QueryAggregatorDiff();
          break;
        case DIV:
          QueryAggregatorDiv divAggregator = new QueryAggregatorDiv();
          JsonElement divisorEle = jsonElement.getAsJsonObject().get("divisor");
          if (divisorEle == null) {
            throw new JsonParseException("Among aggregator div, [divisor] must be specified");
          }
          divAggregator.setDivisor(divisorEle.getAsDouble());
          aggregator = divAggregator;
          break;
        case RATE:
          QueryAggregatorRate rateAggregator = new QueryAggregatorRate();
          JsonElement samplingEle = jsonElement.getAsJsonObject().get(SAMPLING);
          if (samplingEle == null) {
            throw new JsonParseException("Among aggregator rate, [sampling] must be specified");
          }
          JsonElement unitEle = samplingEle.getAsJsonObject().get("unit");
          if (unitEle == null) {
            throw new JsonParseException("Among aggregator rate, [sampling.unit] must be specified");
          }
          rateAggregator.setUnit(unitEle.getAsString());
          aggregator = rateAggregator;
          break;
        case SAMPLER:
          QueryAggregatorSampler samplerAggregator = new QueryAggregatorSampler();
          JsonElement unit = jsonElement.getAsJsonObject().get("unit");
          if (unit == null) {
            throw new JsonParseException("Among aggregator sampler, [unit] must be specified");
          }
          samplerAggregator.setUnit(unit.getAsString());
          aggregator = samplerAggregator;
          break;
        case SAVE_AS:
          QueryAggregatorSaveAs saveAsAggregator = new QueryAggregatorSaveAs();
          JsonElement metricNameEle = jsonElement.getAsJsonObject().get("metric_name");
          if (metricNameEle == null) {
            throw new JsonParseException("Among aggregator save_as, [metric_name] must be specified");
          }
          saveAsAggregator.setMetricName(metricNameEle.getAsString());
          aggregator = saveAsAggregator;
          break;
        case FILTER:
          QueryAggregatorFilter filterAggregator = new QueryAggregatorFilter();
          JsonElement filterOperandEle = jsonElement.getAsJsonObject().get("filter_op");
          if (filterOperandEle == null) {
            throw new JsonParseException("Among aggregator filter, [filter_op] must be specified");
          }
          filterAggregator.setOperandType(filterOperandEle.getAsString());
          JsonElement thresholdEle = jsonElement.getAsJsonObject().get("threshold");
          if (thresholdEle == null) {
            throw new JsonParseException("Among aggregator filter, [threshold] must be specified");
          }
          filterAggregator.setThreshold(thresholdEle.getAsDouble());
          aggregator = filterAggregator;
          break;
        default:
          LOGGER.error("QueryAggregatorDeserializer.deserialize: unexpected aggregator type");
          throw new JsonParseException("unexpected aggregator type");
      }
    } catch (IllegalArgumentException e) {
      throw new JsonParseException(String.format("Invalid aggregator name [%s]", name));
    }

    return aggregator;
  }

  private String getAggregatorName(JsonElement jsonElement) {
    JsonElement nameElm = jsonElement.getAsJsonObject().get("name");
    if (nameElm == null)
      throw new JsonParseException("Aggregator name must be specified");
    return nameElm.getAsString();
  }

  private QueryAggregatorAlign getAlign(JsonElement jsonElement) {

    JsonElement samplingEle = jsonElement.getAsJsonObject().get("align_sampling");
    JsonElement startTimeEle = jsonElement.getAsJsonObject().get("align_start_time");
    JsonElement endTimeEle = jsonElement.getAsJsonObject().get("align_end_time");

    if (samplingEle != null && samplingEle.getAsBoolean()) {
      return QueryAggregatorAlign.ALIGN_SAMPLING;
    }

    if (startTimeEle != null && startTimeEle.getAsBoolean()) {
      return QueryAggregatorAlign.ALIGN_START_TIME;
    }

    if (endTimeEle != null && endTimeEle.getAsBoolean()) {
      return QueryAggregatorAlign.ALIGN_END_TIME;
    }

    return QueryAggregatorAlign.NO_ALIGN;
  }
}
