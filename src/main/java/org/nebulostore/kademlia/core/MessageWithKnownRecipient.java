package org.nebulostore.kademlia.core;

abstract class MessageWithKnownRecipient extends Message {
  private static final long serialVersionUID = 1L;

  private final NodeInfo mDestNodeInfo;

  public MessageWithKnownRecipient(NodeInfo srcNodeInfo, NodeInfo destNodeInfo) {
    super(srcNodeInfo);
    mDestNodeInfo = destNodeInfo;
  }

  public NodeInfo getDestinationNodeInfo() {
    return mDestNodeInfo;
  }
}
