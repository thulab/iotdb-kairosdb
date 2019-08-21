package cn.edu.tsinghua.iotdb.kairosdb.http.rest;

import static cn.edu.tsinghua.iotdb.kairosdb.http.rest.MetricsResource.setHeaders;

import cn.edu.tsinghua.iotdb.kairosdb.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.kairosdb.dao.IoTDBConnectionPool;
import java.sql.Connection;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

@Path("/api/v1/health")
public class HealthCheckResource {

  @GET
  @Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
  @Path("check")
  public Response check() {
    boolean health = true;
    try {
      for (String url : ConfigDescriptor.getInstance().getConfig().URL_LIST) {
        Connection conn = IoTDBConnectionPool.getConnection(url) ;
        conn.close();
      }
    } catch (Exception e) {
      health = false;
    }
    if (health) {
      return setHeaders(Response.status(Response.Status.NO_CONTENT)).build();
    } else {
      return setHeaders(Response.status(Status.INTERNAL_SERVER_ERROR)).build();
    }
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
  @Path("status")
  public Response status() {
    String status = "OK";
    try {
      for (String url : ConfigDescriptor.getInstance().getConfig().URL_LIST) {
        Connection conn = IoTDBConnectionPool.getConnection(url) ;
        conn.close();
      }
    } catch (Exception e) {
      status = "NOT OK";
    }
    String body = String.format("[\"JVM-Thread-Deadlock: OK\",\"Datastore-Query: %s\"]", status);
    Response.ResponseBuilder responseBuilder = Response.status(Status.OK)
        .entity(body);
    return setHeaders(responseBuilder).build();
  }

}
