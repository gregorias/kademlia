package org.nebulostore.kademlia;

abstract class MessageWithKnownRecipient extends Message {
	private static final long serialVersionUID = 1L;

	private final NodeInfo destNodeInfo_;

	public MessageWithKnownRecipient(NodeInfo srcNodeInfo, NodeInfo destNodeInfo) {
		super(srcNodeInfo);
		destNodeInfo_ = destNodeInfo;
	}
	
	public NodeInfo getDestinationNodeInfo() {
		return destNodeInfo_;
	}
}
