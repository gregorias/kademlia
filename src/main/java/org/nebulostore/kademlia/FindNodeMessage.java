package org.nebulostore.kademlia;

class FindNodeMessage extends MessageWithKnownRecipient {
	private static final long serialVersionUID = 1L;

	private final Key key_;

	public FindNodeMessage(NodeInfo srcNodeInfo, NodeInfo destNodeInfo, Key searchedKey) {
		super(srcNodeInfo, destNodeInfo);
		key_ = searchedKey;
	}
	
	public Key getSearchedKey() {
		return key_;
	}
}
