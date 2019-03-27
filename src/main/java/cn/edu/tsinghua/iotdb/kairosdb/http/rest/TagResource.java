package cn.edu.tsinghua.iotdb.kairosdb.http.rest;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/api/v1")
public class TagResource {

  @GET
  @Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
  @Path("tagnames")
  public Response getTagNames() {
    return null;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
  @Path("tagvalues")
  public Response getTagValues() {
    return null;
  }

}
