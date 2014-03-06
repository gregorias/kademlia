package org.nebulostore.kademlia.interfaces.rest;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.nebulostore.kademlia.core.KademliaRouting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("stop")
public final class KademliaStopResource {
	private static final Logger LOGGER = LoggerFactory.getLogger(KademliaStopResource.class);
	private final KademliaRouting kademlia_;
	
	public KademliaStopResource(KademliaRouting kademlia) {
		kademlia_ = kademlia;
	}
	
	@GET
	@Produces(MediaType.TEXT_PLAIN) 
	public Response stop() {
		LOGGER.info("stop()");
        try {
            kademlia_.stop();
        } catch (Exception e) {
			LOGGER.info("stop() -> bad request");
            return Response.status(Response.Status.BAD_REQUEST).entity(
            		String.format("Could not stop kademlia: %s.", e)).build();
        }
		LOGGER.info("stop() -> ok");
        return Response.ok().build();
	}
}
