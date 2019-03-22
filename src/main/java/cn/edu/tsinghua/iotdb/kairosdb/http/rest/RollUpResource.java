package cn.edu.tsinghua.iotdb.kairosdb.http.rest;

import cn.edu.tsinghua.iotdb.kairosdb.http.rest.json.ErrorResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import static cn.edu.tsinghua.iotdb.kairosdb.util.Preconditions.checkNotNullOrEmpty;

@Path("/api/v1/rollups")
public class RollUpResource {
    private static final Logger logger = LoggerFactory.getLogger(RollUpResource.class);

    @POST
    @Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
    public Response create(String json) {
        checkNotNullOrEmpty(json);
        try {

            return null;
        } catch (Exception e) {
            logger.error("Failed to add roll-up.", e);
            return MetricsResource.setHeaders(Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(new ErrorResponse(e.getMessage()))).build();
        }
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
    public Response list() {
        return null;
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
    @Path("{id}")
    public Response get(@PathParam("id") String id) {
        checkNotNullOrEmpty(id);
        return null;
    }

    @DELETE
    @Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
    @Path("{id}")
    public Response delete(@PathParam("id") String id) {
        checkNotNullOrEmpty(id);
        return null;
    }

    @PUT
    @Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
    @Path("{id}")
    public Response update(@PathParam("id") String id, String json) {
        checkNotNullOrEmpty(id);
        checkNotNullOrEmpty(json);
        return null;
    }

}
