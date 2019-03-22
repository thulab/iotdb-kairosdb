package cn.edu.tsinghua.iotdb.kairosdb.http.rest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/api/v1/metadata")
public class MetadataResource {

    private static final Logger logger = LoggerFactory.getLogger(MetadataResource.class);

    @POST
    @Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
    @Path("/{service}/{serviceKey}/{key}")
    public Response setValue(@PathParam("service") String service, @PathParam("serviceKey") String serviceKey,
                             @PathParam("key") String key, String value) {
        return null;
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
    @Path("/{service}/{serviceKey}/{key}")
    public Response getValue(@PathParam("service") String service, @PathParam("serviceKey") String serviceKey,
                             @PathParam("key") String key) {
        return null;
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
    @Path("/{service}")
    public Response listServiceKeys(@PathParam("service") String service) {
        return null;
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
    @Path("/{service}/{serviceKey}")
    public Response listKeys(@PathParam("service") String service, @PathParam("serviceKey") String serviceKey) {
        return null;
    }

    @DELETE
    @Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
    @Path("/{service}/{serviceKey}/{key}")
    public Response deleteKey(@PathParam("service") String service, @PathParam("serviceKey") String serviceKey,
                              @PathParam("key") String key) {
        return null;
    }

}
