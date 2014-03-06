package org.nebulostore.kademlia.core;

import java.io.Serializable;

abstract class Message implements Serializable {
	private static final long serialVersionUID = 1L;

	private final NodeInfo srcNodeInfo_;
	Message(NodeInfo src) {
		srcNodeInfo_ = src;
	}
	
	public NodeInfo getSourceNodeInfo() {
		return srcNodeInfo_;
	}
}
