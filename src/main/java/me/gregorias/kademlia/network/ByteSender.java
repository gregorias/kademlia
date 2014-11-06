package me.gregorias.kademlia.network;

import java.net.InetSocketAddress;

/**
 * Sender of messages.
 *
 * @author Grzegorz Milka
 */
public interface ByteSender {
  /**
   * Sends message and expects response to it.
   *
   * @param destination Destination address
   * @param message
   * @param handler
   */
  void sendMessageWithReply(InetSocketAddress destination,
      byte[] message,
      ByteResponseHandler handler);
}