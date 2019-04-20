package cn.edu.tsinghua.iotdb.kairosdb.query.group_by;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import java.lang.reflect.Type;

public class GroupBySerializer implements JsonSerializer<GroupBy> {

  @Override
  public JsonElement serialize(GroupBy groupBy, Type type, JsonSerializationContext context) {
    switch (groupBy.getKind()) {
      case TYPE:
        GroupByType groupByType = (GroupByType) groupBy;
        JsonObject obj = new JsonObject();
        obj.addProperty("name", "type");
        obj.addProperty("type", groupByType.getType());
        return obj;
      case BIN:
        break;
      case TAGS:
        break;
      case TIME:
        break;
      case VALUE:
        break;
      default:
        break;
    }

    return null;
  }

}
