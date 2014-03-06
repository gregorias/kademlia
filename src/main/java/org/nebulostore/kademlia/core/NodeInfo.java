package org.nebulostore.kademlia.core;

import java.io.Serializable;
import java.net.InetSocketAddress;

/**
 * Pair of {@link Key} and associated socket address of a node.
 * 
 * @author Grzegorz Milka
 */
public class NodeInfo implements Serializable {
	private static final long serialVersionUID = 1L;

	private final Key key_;
	private final InetSocketAddress socketAddress_;
	
	public NodeInfo(Key key, InetSocketAddress socketAddress) {
		key_ = key;
		socketAddress_ = socketAddress;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof NodeInfo)) {
			return false;
		}
		NodeInfo other = (NodeInfo) obj;
		if (key_ == null) {
			if (other.key_ != null) {
				return false;
			}
		} else if (!key_.equals(other.key_)) {
			return false;
		}
		if (socketAddress_ == null) {
			if (other.socketAddress_ != null) {
				return false;
			}
		} else if (!socketAddress_.equals(other.socketAddress_)) {
			return false;
		}
		return true;
	}

	public Key getKey() {
		return key_;
	}
	
	public InetSocketAddress getSocketAddress() {
		return socketAddress_;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((key_ == null) ? 0 : key_.hashCode());
		result = prime * result
				+ ((socketAddress_ == null) ? 0 : socketAddress_.hashCode());
		return result;
	}
	
	@Override
	public String toString() {
		return String.format("NodeInfo[key: %s, address: %s]", key_, socketAddress_);
	}
}
