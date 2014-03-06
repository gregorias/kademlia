package org.nebulostore.kademlia.interfaces.rest;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.nebulostore.kademlia.core.KademliaRouting;

@Path("start")
public final class KademliaStartResource {
	private final KademliaRouting kademlia_;
	
	public KademliaStartResource(KademliaRouting kademlia) {
		kademlia_ = kademlia;
	}
	
	@GET
	@Produces(MediaType.TEXT_PLAIN) 
	public Response stop() {
        try {
            kademlia_.start();
        } catch (Exception e) {
            return Response.status(Response.Status.BAD_REQUEST).entity(
            		String.format("Could not start up kademlia: %s.", e)).build();
        }
        return Response.ok().build();
	}
}
