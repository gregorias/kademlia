package me.gregorias.kademlia.core;

class FindNodeMessage extends MessageWithKnownRecipient {
  private static final long serialVersionUID = 1L;

  private final Key mKey;

  public FindNodeMessage(NodeInfo srcNodeInfo, NodeInfo destNodeInfo, Key searchedKey) {
    super(srcNodeInfo, destNodeInfo);
    mKey = searchedKey;
  }

  public Key getSearchedKey() {
    return mKey;
  }
}
