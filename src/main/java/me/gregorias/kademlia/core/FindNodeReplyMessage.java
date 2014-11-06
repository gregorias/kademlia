package me.gregorias.kademlia.core;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;

class FindNodeReplyMessage extends MessageWithKnownRecipient {
  private static final long serialVersionUID = 1L;

  private final LinkedList<NodeInfo> mFoundNodes;

  public FindNodeReplyMessage(NodeInfo srcNodeInfo, NodeInfo destNodeInfo,
      Collection<NodeInfo> foundNodes) {
    super(srcNodeInfo, destNodeInfo);
    mFoundNodes = new LinkedList<NodeInfo>(foundNodes);
  }

  public Collection<NodeInfo> getFoundNodes() {
    return new ArrayList<>(mFoundNodes);
  }
}
