package cn.edu.tsinghua.iotdb.kairosdb.http.rest;

import cn.edu.tsinghua.iotdb.kairosdb.http.rest.json.JsonResponseBuilder;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/myresource")
public class MyResource {

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getIt() {
    return new JsonResponseBuilder(Response.Status.OK)
        .addError("Hello World!!!")
        .buildPlainText();
  }

}