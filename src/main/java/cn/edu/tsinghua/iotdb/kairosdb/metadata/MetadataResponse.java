package cn.edu.tsinghua.iotdb.kairosdb.metadata;

import java.util.List;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

public class MetadataResponse {

  private MetadataResponse() {
  }

  public static Response getResponse() {
    return Response.status(Status.NO_CONTENT).build();
  }

  public static Response getResponse(String message) {
    return Response.status(Status.OK).entity(message).build();
  }

  public static Response getResponse(List<String> message) {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("{\"results\":[");
    for (String msg : message) {
      stringBuilder.append("\"");
      stringBuilder.append(msg);
      stringBuilder.append("\",");
    }
    if (!message.isEmpty()) {
      stringBuilder.deleteCharAt(stringBuilder.length()-1);
    }
    stringBuilder.append("]}");

    return Response.status(Status.OK).entity(stringBuilder.toString()).build();
  }

}
