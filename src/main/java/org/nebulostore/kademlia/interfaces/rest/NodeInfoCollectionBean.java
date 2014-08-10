package org.nebulostore.kademlia.interfaces.rest;

public final class NodeInfoCollectionBean {
    private NodeInfoBean[] nodeInfos_;

    public NodeInfoBean[] getNodeInfo() {
        return nodeInfos_;
    }

    public void setNodeInfo(NodeInfoBean[] nodeInfos) {
        nodeInfos_ = nodeInfos;
    }
}
