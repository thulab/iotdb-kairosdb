package cn.edu.tsinghua.iotdb.kairosdb.http.rest;

import cn.edu.tsinghua.iotdb.kairosdb.conf.Config;
import cn.edu.tsinghua.iotdb.kairosdb.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.kairosdb.dao.IngestionWorker;
import cn.edu.tsinghua.iotdb.kairosdb.dao.MetricsManager;
import cn.edu.tsinghua.iotdb.kairosdb.http.rest.json.JsonResponseBuilder;
import cn.edu.tsinghua.iotdb.kairosdb.profile.Measurement;
import cn.edu.tsinghua.iotdb.kairosdb.profile.Measurement.Profile;
import cn.edu.tsinghua.iotdb.kairosdb.query.Query;
import cn.edu.tsinghua.iotdb.kairosdb.query.QueryException;
import cn.edu.tsinghua.iotdb.kairosdb.query.QueryExecutor;
import cn.edu.tsinghua.iotdb.kairosdb.query.QueryParser;
import com.alibaba.fastjson.JSON;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/api/v1")
public class MetricsResource {

  private static final String QUERY_URL = "/datapoints/query";
  private static final String NO_CACHE = "no-cache";
  private static final ExecutorService threadPool = Executors.newCachedThreadPool();
  private static final ScheduledExecutorService profilerPool = Executors.newSingleThreadScheduledExecutor();
  private static final Config config = ConfigDescriptor.getInstance().getConfig();
  public static final Logger LOGGER = LoggerFactory.getLogger(MetricsResource.class);

  //Used for parsing incoming metrics
  private final Gson gson;

  @Inject
  public MetricsResource() {
    GsonBuilder builder = new GsonBuilder();
    gson = builder.disableHtmlEscaping().create();
    if (config.ENABLE_PROFILER) {
      profilerPool.scheduleAtFixedRate(() -> Measurement.getInstance().show(),
          config.PROFILE_INTERVAL, config.PROFILE_INTERVAL, TimeUnit.SECONDS);
    }
  }

  static Response.ResponseBuilder setHeaders(Response.ResponseBuilder responseBuilder) {
    responseBuilder.header("Access-Control-Allow-Origin", "*");
    responseBuilder.header("Pragma", NO_CACHE);
    responseBuilder.header("Cache-Control", NO_CACHE);
    responseBuilder.header("Expires", 0);
    return (responseBuilder);
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
  @Consumes("application/gzip")
  @Path("/datapoints")
  public void addGzip(InputStream gzip, @Suspended final AsyncResponse asyncResponse) {
    GZIPInputStream gzipInputStream;
    try {
      gzipInputStream = new GZIPInputStream(gzip);
      add(null, gzipInputStream, asyncResponse);
    } catch (IOException e) {
      JsonResponseBuilder builder = new JsonResponseBuilder(Response.Status.BAD_REQUEST);
      asyncResponse.resume(builder.addError(e.getMessage()).build());
    }
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
  @Path("/datapoints")
  public void add(@Context HttpHeaders httpheaders, final InputStream stream,
      @Suspended final AsyncResponse asyncResponse) {
    threadPool.execute(new IngestionWorker(asyncResponse, httpheaders, stream, gson));
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
  @Path("/datapoints/delete")
  public Response delete(String queryJson) {
    if (queryJson == null) {
      return setHeaders(Response.status(Status.BAD_REQUEST)).build();
    }
    try {
      QueryParser parser = new QueryParser();
      Query query = parser.parseQueryMetric(queryJson);
      new QueryExecutor(query).delete();
    } catch (QueryException | BeanValidationException e) {
      return setHeaders(Response.status(Status.BAD_REQUEST)).build();
    }

    return setHeaders(Response.status(Status.NO_CONTENT)).build();
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
  @Path(QUERY_URL)
  public Response getQuery(@QueryParam("query") String json) {
    return runQuery(json);
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
  @Path(QUERY_URL)
  public Response postQuery(String json) {
    return runQuery(json);
  }

//  @POST
//  @Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
//  @Path(QUERY_URL)
//  public void postQuery(String json, @Suspended final AsyncResponse asyncResponse) {
//    CompletableFuture<Response> future = veryExpensiveOperation(json);
//    future.thenAccept(resp -> asyncResponse.resume(resp));
//  }

  private CompletableFuture<Response> veryExpensiveOperation(String json){
    CompletableFuture<Response> completableFuture = new CompletableFuture<>();
    threadPool.execute(new Thread(() -> {
      Response response = null;
      //do expensive stuff here
      try {
        response = runQuery(json);
      } catch (Exception e) {
        LOGGER.error("Query failed because: {}", e.getMessage(), e);
      }
      completableFuture.complete(response);
    }));
    return completableFuture;
  }

  private Response runQuery(String jsonStr) {
    long start = 0;
    if (config.ENABLE_PROFILER) {
      start = System.nanoTime();
    }
    try {
      if (jsonStr == null) {
        throw new BeanValidationException(
            new QueryParser.SimpleConstraintViolation("query json", "must not be null or empty"),
            "");
      }

      QueryParser parser = new QueryParser();
      Query query = parser.parseQueryMetric(jsonStr);
      QueryExecutor executor = new QueryExecutor(query);
      String entity = executor.executeV2();
      if (config.ENABLE_PROFILER) {
        Measurement.getInstance().add(Profile.IKR_QUERY, System.nanoTime() - start);
      }

      return Response.status(Status.OK)
          .header("Access-Control-Allow-Origin", "*")
          .header("Pragma", NO_CACHE)
          .header("Cache-Control", NO_CACHE)
          .header("Expires", 0)
//          .header("Vary","Accept-Encoding, User-Agent")
//          .header("Transfer-Encoding","chunked")
//          .header("Server","Jetty(8.1.16.v20140903)")
          .entity(entity)
          .build();

    } catch (BeanValidationException e) {
      JsonResponseBuilder builder = new JsonResponseBuilder(Response.Status.BAD_REQUEST);
      return builder.addErrors(e.getErrorMessages()).build();
    } catch (QueryException e) {
      JsonResponseBuilder builder = new JsonResponseBuilder(Response.Status.BAD_REQUEST);
      return builder.addError(e.getMessage()).build();
    }
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
  @Path("/metricnames")
  public Response getMetricNames(@QueryParam("prefix") String prefix) {
    Map<String, Object> metricNameResultMap = new HashMap<>();
    List<String> metricNameResultList = MetricsManager.getMetricNamesList(prefix);
    metricNameResultMap.put("results", metricNameResultList);
    String result = JSON.toJSONString(metricNameResultMap);
    Response.ResponseBuilder responseBuilder = Response.status(Response.Status.OK).entity(result);
    setHeaders(responseBuilder);
    return responseBuilder.build();
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
  @Path("/version")
  public Response getVersion() {
    Response.ResponseBuilder responseBuilder = Response.status(Response.Status.OK)
        .entity("{\"version\": \"1.0.0\"}\n");
    setHeaders(responseBuilder);
    return responseBuilder.build();
  }

  @DELETE
  @Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
  @Path("/metric/{metricName}")
  public Response metricDelete(@PathParam("metricName") String metricName) {
    MetricsManager.deleteMetric(metricName);
    return setHeaders(Response.status(Response.Status.NO_CONTENT)).build();
  }

}
