package cn.edu.tsinghua.iotdb.kairosdb.http.rest;

import static cn.edu.tsinghua.iotdb.kairosdb.http.rest.MetricsResource.setHeaders;
import static cn.edu.tsinghua.iotdb.kairosdb.util.Preconditions.checkNotNullOrEmpty;

import cn.edu.tsinghua.iotdb.kairosdb.http.rest.json.ErrorResponse;
import cn.edu.tsinghua.iotdb.kairosdb.http.rest.json.JsonResponseBuilder;
import cn.edu.tsinghua.iotdb.kairosdb.rollup.RollUp;
import cn.edu.tsinghua.iotdb.kairosdb.rollup.RollUpParser;
import cn.edu.tsinghua.iotdb.kairosdb.rollup.RollUpStore;
import cn.edu.tsinghua.iotdb.kairosdb.rollup.RollUpStoreImpl;
import cn.edu.tsinghua.iotdb.kairosdb.rollup.RollUpsExecutor;
import cn.edu.tsinghua.iotdb.kairosdb.rollup.RollUpResponse;
import java.util.Map;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/api/v1/rollups")
public class RollUpResource {

  private static final Logger logger = LoggerFactory.getLogger(RollUpResource.class);
  private final RollUpParser parser = new RollUpParser();
  private RollUpStore store = new RollUpStoreImpl();
  private static final String RESOURCE_URL = "/api/v1/rollups/";
  private static final String RESOURCE_NOT_FOUND = "Resource not found for id ";

  @POST
  @Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
  public Response create(String json) {
    checkNotNullOrEmpty(json);
    try {
      String currTaskId = String.valueOf(System.currentTimeMillis());
      RollUp task = parser.parseRollupTask(json, currTaskId);
      RollUpsExecutor.getInstance().create(task);
      store.write(json, currTaskId);
      ResponseBuilder responseBuilder = Response.status(Status.OK)
          .entity(parser.getGson().toJson(createResponse(task)));
      setHeaders(responseBuilder);
      return responseBuilder.build();
    } catch (Exception e) {
      logger.error("Failed to add roll-up.", e);
      return MetricsResource.setHeaders(Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity(new ErrorResponse(e.getMessage()))).build();
    }
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
  public Response list() {
    try {
      Map<String, RollUp> tasks = store.read();

      StringBuilder json = new StringBuilder();
      json.append('[');
      for (RollUp rollUp : tasks.values()) {
        json.append("{\"id\":\"").append(rollUp.getId()).append("\",");
        json.append(rollUp.getJson().substring(1)).append(",");
      }

      if (json.length() > 1) {
        json.deleteCharAt(json.length() - 1);
      }
      json.append(']');

      ResponseBuilder responseBuilder = Response.status(Status.OK).entity(json.toString());
      setHeaders(responseBuilder);
      return responseBuilder.build();
    } catch (Exception e) {
      logger.error("Failed to list roll-ups.", e);
      JsonResponseBuilder builder = new JsonResponseBuilder(Status.INTERNAL_SERVER_ERROR);
      return builder.addError(e.getMessage()).build();
    }
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
  @Path("{id}")
  public Response get(@PathParam("id") String id) {
    checkNotNullOrEmpty(id);
    try {
      ResponseBuilder responseBuilder;
      if (RollUpsExecutor.getInstance().containsRollup(id)) {
        RollUp task = store.read(id);
        String json = "{\"id\":\"" + task.getId() + "\"," + task.getJson().substring(1);
        responseBuilder = Response.status(Status.OK).entity(json);
      } else {
        JsonResponseBuilder builder = new JsonResponseBuilder(Status.BAD_REQUEST);
        return builder.addError(RESOURCE_NOT_FOUND + id).build();
      }
      setHeaders(responseBuilder);
      return responseBuilder.build();
    } catch (Exception e) {
      logger.error("Failed to get roll-up.", e);
      JsonResponseBuilder builder = new JsonResponseBuilder(Status.INTERNAL_SERVER_ERROR);
      return builder.addError(e.getMessage()).build();
    }
  }

  @DELETE
  @Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
  @Path("{id}")
  public Response delete(@PathParam("id") String id) {
    try {
      checkNotNullOrEmpty(id);
      if (RollUpsExecutor.getInstance().containsRollup(id)) {
        RollUpsExecutor.getInstance().delete(id);
        store.remove(id);
        return setHeaders(Response.status(Status.NO_CONTENT)).build();
      } else {
        JsonResponseBuilder builder = new JsonResponseBuilder(Status.BAD_REQUEST);
        return builder.addError(RESOURCE_NOT_FOUND + id).build();
      }
    } catch (Exception e) {
      logger.error("Failed to delete roll-up.", e);
      JsonResponseBuilder builder = new JsonResponseBuilder(Status.INTERNAL_SERVER_ERROR);
      return builder.addError(e.getMessage()).build();
    }
  }

  @PUT
  @Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
  @Path("{id}")
  public Response update(@PathParam("id") String id, String json) {
    checkNotNullOrEmpty(id);
    checkNotNullOrEmpty(json);
    ResponseBuilder responseBuilder;
    try {
      if (RollUpsExecutor.getInstance().containsRollup(id)) {
        RollUp rollUp = parser.parseRollupTask(json, id);
        RollUpsExecutor.getInstance().update(rollUp);
        store.write(json, id);
        responseBuilder = Response.status(Status.OK)
            .entity(parser.getGson().toJson(createResponse(rollUp)));
      } else {
        responseBuilder = Response.status(Status.NOT_FOUND)
            .entity(new ErrorResponse(RESOURCE_NOT_FOUND + id));
      }
      return responseBuilder.build();
    } catch (Exception e) {
      logger.error("Failed to update roll-up.", e);
      JsonResponseBuilder builder = new JsonResponseBuilder(Status.INTERNAL_SERVER_ERROR);
      return builder.addError(e.getMessage()).build();
    }
  }

  private RollUpResponse createResponse(RollUp task) {
    return new RollUpResponse(task.getId(), task.getName(), RESOURCE_URL + task.getId());
  }

}
