package org.nebulostore.kademlia.interfaces.rest;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.nebulostore.kademlia.KademliaRouting;

@Path("get_key")
public final class GetLocalKeyResource {
	private final KademliaRouting kademlia_;
	
	public GetLocalKeyResource(KademliaRouting kademlia) {
		kademlia_ = kademlia;
	}
	
	@GET
	@Produces(MediaType.TEXT_PLAIN) 
	public String getKey() {
		return kademlia_.getLocalKey().toString();
	}
}
