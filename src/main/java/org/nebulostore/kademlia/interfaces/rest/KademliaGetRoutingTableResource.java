package org.nebulostore.kademlia.interfaces.rest;

import java.util.Collection;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.glassfish.jersey.server.JSONP;
import org.nebulostore.kademlia.core.KademliaRouting;
import org.nebulostore.kademlia.core.NodeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("get_routing_table")
public final class KademliaGetRoutingTableResource {
  private static final Logger LOGGER = LoggerFactory
      .getLogger(KademliaGetRoutingTableResource.class);
  private final KademliaRouting kademlia_;

  public KademliaGetRoutingTableResource(KademliaRouting kademlia) {
    kademlia_ = kademlia;
  }

  @GET
  @JSONP
  @Produces(MediaType.APPLICATION_JSON)
  public Response getRoutingTable() {
    LOGGER.info("getRoutingTable()");
    Collection<NodeInfo> nodeInfos;
    try {
      nodeInfos = kademlia_.getRoutingTable();
    } catch (Exception e) {
      LOGGER.info("getRoutingTable() -> bad request");
      return Response
          .status(Response.Status.BAD_REQUEST)
          .entity(
              String
                  .format("Could not get routing table from kademlia: %s.", e))
          .build();
    }
    NodeInfoBean[] parsedNodeInfos = new NodeInfoBean[nodeInfos.size()];
    int i = 0;
    for (NodeInfo nodeInfo : nodeInfos) {
      parsedNodeInfos[i] = NodeInfoBean.fromNodeInfo(nodeInfo);
      ++i;
    }
    NodeInfoCollectionBean bean = new NodeInfoCollectionBean();
    bean.setNodeInfo(parsedNodeInfos);
    LOGGER.info("getRoutingTable() -> ok");
    return Response.ok(bean).build();
  }
}