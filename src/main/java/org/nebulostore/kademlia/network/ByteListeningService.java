package org.nebulostore.kademlia.network;

/**
 * Observable service which listens for messages on the network.
 *
 * @author Grzegorz Milka
 */
public interface ByteListeningService {
  void registerListener(ByteListener listener);

  void unregisterListener(ByteListener listener);
}
