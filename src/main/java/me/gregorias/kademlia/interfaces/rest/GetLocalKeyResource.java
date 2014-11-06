package me.gregorias.kademlia.interfaces.rest;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import me.gregorias.kademlia.core.KademliaRouting;

@Path("get_key")
public final class GetLocalKeyResource {
  private final KademliaRouting mKademlia;

  public GetLocalKeyResource(KademliaRouting kademlia) {
    mKademlia = kademlia;
  }

  @GET
  @Produces(MediaType.TEXT_PLAIN)
  public String getKey() {
    return mKademlia.getLocalKey().toString();
  }
}
