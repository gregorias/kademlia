package me.gregorias.kademlia.core;

class PongMessage extends MessageWithKnownRecipient {
  private static final long serialVersionUID = 1L;

  public PongMessage(NodeInfo srcNodeInfo, NodeInfo destNodeInfo) {
    super(srcNodeInfo, destNodeInfo);
  }
}
