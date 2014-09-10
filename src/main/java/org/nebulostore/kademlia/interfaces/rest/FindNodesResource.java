package org.nebulostore.kademlia.interfaces.rest;

import java.util.Collection;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.glassfish.jersey.server.JSONP;
import org.nebulostore.kademlia.core.KademliaException;
import org.nebulostore.kademlia.core.KademliaRouting;
import org.nebulostore.kademlia.core.Key;
import org.nebulostore.kademlia.core.NodeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("find_nodes/{key}")
public final class FindNodesResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(FindNodesResource.class);
  private final KademliaRouting mKademlia;

  public FindNodesResource(KademliaRouting kademlia) {
    mKademlia = kademlia;
  }

  @POST
  @JSONP
  @Produces(MediaType.APPLICATION_JSON)
  public NodeInfoCollectionBean findNodes(@PathParam("key") String paramKey) {
    LOGGER.info("findNodes({})", paramKey);
    Key key = new Key(Integer.parseInt(paramKey));
    Collection<NodeInfo> nodeInfos = null;
    try {
      nodeInfos = mKademlia.findClosestNodes(key);
    } catch (InterruptedException | KademliaException e) {
      LOGGER.error(String.format("findNodes(%s)", key), e);
      return null;
    }
    NodeInfoBean[] parsedNodeInfos = new NodeInfoBean[nodeInfos.size()];
    int idx = 0;
    for (NodeInfo nodeInfo : nodeInfos) {
      parsedNodeInfos[idx] = NodeInfoBean.fromNodeInfo(nodeInfo);
      ++idx;
    }
    NodeInfoCollectionBean bean = new NodeInfoCollectionBean();
    bean.setNodeInfo(parsedNodeInfos);
    return bean;
  }
}
