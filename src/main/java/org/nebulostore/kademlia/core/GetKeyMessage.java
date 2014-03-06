package org.nebulostore.kademlia.core;

class GetKeyMessage extends Message {
	private static final long serialVersionUID = 1L;

	public GetKeyMessage(NodeInfo localNodeInfo) {
		super(localNodeInfo);
	}
}
