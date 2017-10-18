package org.icc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/service")
public class IngestService {

    private static final Logger LOGGER = LoggerFactory.getLogger(IngestService.class);
    @POST
    @Path("/ingest")
    @Produces(MediaType.TEXT_PLAIN)
    public Response ingest(String payload) {
        try {
            JobQueueServer.getProducer().getWriter().send(payload.toString());
        } catch (Exception e) {
            LOGGER.error("Error in ingesting message to kafka : {}", e.getMessage());
            return Response.ok().entity("Failed").build();
        }
        return Response.ok().entity("Success").build();
    }

}