package org.nebulostore.kademlia;

class PingMessage extends MessageWithKnownRecipient {
	private static final long serialVersionUID = 1L;

	public PingMessage(NodeInfo srcNodeInfo, NodeInfo destNodeInfo) {
		super(srcNodeInfo, destNodeInfo);
	}
}
