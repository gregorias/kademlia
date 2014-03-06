package org.nebulostore.kademlia.interfaces.rest;

public final class NodeInfoCollectionBean {
    private String[] nodeInfos_;

    public String[] getNodeInfo() {
        return nodeInfos_;
    }

    public void setNodeInfo(String[] nodeInfos) {
        nodeInfos_ = nodeInfos;
    }
}
