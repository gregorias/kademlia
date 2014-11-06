package me.gregorias.kademlia.interfaces.rest;

public final class NodeInfoCollectionBean {
  private NodeInfoBean[] mNodeInfos;

  public NodeInfoBean[] getNodeInfo() {
    return mNodeInfos;
  }

  public void setNodeInfo(NodeInfoBean[] nodeInfos) {
    mNodeInfos = nodeInfos;
  }
}
