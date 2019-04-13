package cn.edu.tsinghua.iotdb.kairosdb.http.rest.json;

import cn.edu.tsinghua.iotdb.kairosdb.datastore.TimeUnit;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import java.lang.reflect.Type;

public class TimeUnitDeserializer implements JsonDeserializer<TimeUnit> {

  @Override
  public TimeUnit deserialize(JsonElement jsonElement, Type type,
      JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {
    return TimeUnit.from(jsonElement.getAsString());
  }

}
