package cn.edu.tsinghua.iotdb.kairosdb.http.rest;

import cn.edu.tsinghua.iotdb.kairosdb.dao.MetricsManager;
import cn.edu.tsinghua.iotdb.kairosdb.http.rest.json.DataPointsParser;
import cn.edu.tsinghua.iotdb.kairosdb.http.rest.json.ErrorResponse;
import cn.edu.tsinghua.iotdb.kairosdb.http.rest.json.JsonResponseBuilder;
import cn.edu.tsinghua.iotdb.kairosdb.http.rest.json.ValidationErrors;
import cn.edu.tsinghua.iotdb.kairosdb.query.Query;
import cn.edu.tsinghua.iotdb.kairosdb.query.QueryException;
import cn.edu.tsinghua.iotdb.kairosdb.query.QueryExecutor;
import cn.edu.tsinghua.iotdb.kairosdb.query.QueryParser;
import cn.edu.tsinghua.iotdb.kairosdb.query.result.QueryResult;
import com.alibaba.fastjson.JSON;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;
import com.google.gson.stream.MalformedJsonException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
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
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/api/v1")
public class MetricsResource {

  private static final Logger logger = LoggerFactory.getLogger(MetricsResource.class);

  private static final String QUERY_URL = "/datapoints/query";

  private static final String NO_CACHE = "no-cache";

  //These two are used to track rate of ingestion
  private final AtomicInteger ingestedDataPoints = new AtomicInteger();
  private final AtomicInteger ingestTime = new AtomicInteger();

  //Used for parsing incoming metrics
  private final Gson gson;

  @Inject
  public MetricsResource() {
    GsonBuilder builder = new GsonBuilder();
    gson = builder.disableHtmlEscaping().create();
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
  public Response addGzip(InputStream gzip) {
    GZIPInputStream gzipInputStream;
    try {
      gzipInputStream = new GZIPInputStream(gzip);
    } catch (IOException e) {
      JsonResponseBuilder builder = new JsonResponseBuilder(Response.Status.BAD_REQUEST);
      return builder.addError(e.getMessage()).build();
    }
    return (add(null, gzipInputStream));
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
  @Path("/datapoints")
  public Response add(@Context HttpHeaders httpheaders, InputStream stream) {
    try {
      if (httpheaders != null) {
        List<String> requestHeader = httpheaders.getRequestHeader("Content-Encoding");
        if (requestHeader != null && requestHeader.contains("gzip")) {
          stream = new GZIPInputStream(stream);
        }
      }

      DataPointsParser parser = new DataPointsParser(
          new InputStreamReader(stream, StandardCharsets.UTF_8), gson);
      ValidationErrors validationErrors = parser.parse();

      ingestedDataPoints.addAndGet(parser.getDataPointCount());
      ingestTime.addAndGet(parser.getIngestTime());

      if (!validationErrors.hasErrors()) {
        return setHeaders(Response.status(Response.Status.NO_CONTENT)).build();
      } else {
        JsonResponseBuilder builder = new JsonResponseBuilder(Response.Status.BAD_REQUEST);
        for (String errorMessage : validationErrors.getErrors()) {
          builder.addError(errorMessage);
        }
        return builder.build();
      }
    } catch (JsonIOException | MalformedJsonException | JsonSyntaxException e) {
      JsonResponseBuilder builder = new JsonResponseBuilder(Response.Status.BAD_REQUEST);
      return builder.addError(e.getMessage()).build();
    } catch (Exception e) {
      logger.error("Failed to add metric.", e);
      return setHeaders(Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity(new ErrorResponse(e.getMessage()))).build();

    } catch (OutOfMemoryError e) {
      logger.error("Out of memory error.", e);
      return setHeaders(Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity(new ErrorResponse(e.getMessage()))).build();
    }
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
  @Path("/datapoints/delete")
  public Response delete(String json) {
    return null;
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

  private Response runQuery(String jsonStr) {
    try {
      if (jsonStr == null) {
        throw new BeanValidationException(
            new QueryParser.SimpleConstraintViolation("query json", "must not be null or empty"),
            "");
      }

      QueryParser parser = new QueryParser();
      Query query = parser.parseQueryMetric(jsonStr);
      QueryExecutor executor = new QueryExecutor(query);
      QueryResult result = executor.execute();
      String entity = parser.parseResultToJson(result);
      return Response.status(Status.OK)
          .header("Access-Control-Allow-Origin", "*")
          .header("Pragma", NO_CACHE)
          .header("Cache-Control", NO_CACHE)
          .header("Expires", 0)
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
    // delete the metric
    return setHeaders(Response.status(Response.Status.NO_CONTENT)).build();
  }

}
