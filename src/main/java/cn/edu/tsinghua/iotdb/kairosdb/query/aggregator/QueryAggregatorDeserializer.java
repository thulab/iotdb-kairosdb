package cn.edu.tsinghua.iotdb.kairosdb.query.aggregator;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import java.lang.reflect.Type;

public class QueryAggregatorDeserializer implements JsonDeserializer<QueryAggregator> {

  @Override
  public QueryAggregator deserialize(JsonElement jsonElement, Type type,
      JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {
    return null;
  }
}
