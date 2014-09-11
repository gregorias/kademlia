package org.nebulostore.kademlia.interfaces.rest;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.nebulostore.kademlia.core.KademliaRouting;

/**
 * Start this Kademlia peer.
 *
 * @author Grzegorz Milka
 */
@Path("start")
public final class KademliaStartResource {
  private final KademliaRouting mKademlia;

  public KademliaStartResource(KademliaRouting kademlia) {
    mKademlia = kademlia;
  }

  @POST
  @Produces(MediaType.TEXT_PLAIN)
  public Response stop() {
    try {
      mKademlia.start();
    } catch (Exception e) {
      return Response.status(Response.Status.BAD_REQUEST).
          entity(String.format("Could not start up kademlia: %s.", e)).build();
    }
    return Response.ok().build();
  }
}
