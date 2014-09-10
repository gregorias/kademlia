package org.nebulostore.kademlia.core;

public class KademliaException extends Exception {
  private static final long serialVersionUID = 1L;

  public KademliaException() {
  }

  public KademliaException(String message) {
    super(message);
  }

  public KademliaException(Throwable cause) {
    super(cause);
  }

  public KademliaException(String message, Throwable cause) {
    super(message, cause);
  }

  public KademliaException(String message, Throwable cause, boolean enableSuppression,
      boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }

}
