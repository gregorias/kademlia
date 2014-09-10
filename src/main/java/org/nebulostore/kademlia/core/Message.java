package org.nebulostore.kademlia.core;

import java.io.Serializable;

abstract class Message implements Serializable {
  private static final long serialVersionUID = 1L;

  private final NodeInfo mSrcNodeInfo;

  Message(NodeInfo src) {
    mSrcNodeInfo = src;
  }

  public NodeInfo getSourceNodeInfo() {
    return mSrcNodeInfo;
  }
}
