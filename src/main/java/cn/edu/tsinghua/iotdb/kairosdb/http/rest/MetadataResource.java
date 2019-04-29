package cn.edu.tsinghua.iotdb.kairosdb.http.rest;

import cn.edu.tsinghua.iotdb.kairosdb.dao.MetadataManager;
import cn.edu.tsinghua.iotdb.kairosdb.http.rest.json.JsonResponseBuilder;
import cn.edu.tsinghua.iotdb.kairosdb.metadata.MetadataException;
import cn.edu.tsinghua.iotdb.kairosdb.metadata.MetadataResponse;
import java.util.List;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/api/v1/metadata")
public class MetadataResource {

  private static final Logger logger = LoggerFactory.getLogger(MetadataResource.class);

  @POST
  @Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
  @Path("/{service}/{serviceKey}/{key}")
  public Response setValue(@PathParam("service") String service,
      @PathParam("serviceKey") String serviceKey,
      @PathParam("key") String key,
      String value) {
    MetadataManager manager = MetadataManager.getInstance();
    try {
      manager.addOrUpdateValue(service, serviceKey, key, value);
      return MetadataResponse.getResponse();
    } catch (MetadataException e) {
      logger.error(String.format("%s: %s", e.getClass().getName(), e.getMessage()));
      return new JsonResponseBuilder(Status.INTERNAL_SERVER_ERROR).addError(e.getMessage()).build();
    }
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
  @Path("/{service}/{serviceKey}/{key}")
  public Response getValue(@PathParam("service") String service,
      @PathParam("serviceKey") String serviceKey,
      @PathParam("key") String key) {
    MetadataManager manager = MetadataManager.getInstance();
    String value = manager.getValue(service, serviceKey, key);
    if (value == null) {
      value = "";
    }
    return MetadataResponse.getResponse(value);
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
  @Path("/{service}")
  public Response listServiceKeys(@PathParam("service") String service) {
    MetadataManager manager = MetadataManager.getInstance();
    List<String> list = manager.getServiceKeyList(service);
    return MetadataResponse.getResponse(list);
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
  @Path("/{service}/{serviceKey}")
  public Response listKeys(@PathParam("service") String service,
      @PathParam("serviceKey") String serviceKey) {
    MetadataManager manager = MetadataManager.getInstance();
    List<String> list = manager.getKeyList(service, serviceKey);
    return MetadataResponse.getResponse(list);
  }

  @DELETE
  @Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
  @Path("/{service}/{serviceKey}/{key}")
  public Response deleteKey(@PathParam("service") String service,
      @PathParam("serviceKey") String serviceKey,
      @PathParam("key") String key) {
    MetadataManager manager = MetadataManager.getInstance();
    try {
      manager.deleteValue(service, serviceKey, key);
      return MetadataResponse.getResponse();
    } catch (MetadataException e) {
      logger.error(String.format("%s: %s", e.getClass().getName(), e.getMessage()));
      return new JsonResponseBuilder(Status.INTERNAL_SERVER_ERROR).addError(e.getMessage()).build();
    }
  }

}
