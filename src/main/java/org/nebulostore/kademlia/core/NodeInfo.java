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

  private final Key mKey;
  private final InetSocketAddress mSocketAddress;

  public NodeInfo(Key key, InetSocketAddress socketAddress) {
    mKey = key;
    mSocketAddress = socketAddress;
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
    if (mKey == null) {
      if (other.mKey != null) {
        return false;
      }
    } else if (!mKey.equals(other.mKey)) {
      return false;
    }
    if (mSocketAddress == null) {
      if (other.mSocketAddress != null) {
        return false;
      }
    } else if (!mSocketAddress.equals(other.mSocketAddress)) {
      return false;
    }
    return true;
  }

  public Key getKey() {
    return mKey;
  }

  public InetSocketAddress getSocketAddress() {
    return mSocketAddress;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((mKey == null) ? 0 : mKey.hashCode());
    result = prime * result + ((mSocketAddress == null) ? 0 : mSocketAddress.hashCode());
    return result;
  }

  @Override
  public String toString() {
    return String.format("NodeInfo[key: %s, address: %s]", mKey, mSocketAddress);
  }
}
