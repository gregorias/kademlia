package me.gregorias.kademlia.network;

/**
 * Observer for incoming messages.
 *
 * @author Grzegorz Milka
 */
public interface ByteListener {
  /**
   * Method to be called when a message with expected response is received.
   *
   * @return byte array to be returned to sender or null on error.
   */
  byte[] receiveByteArrayWithResponse(byte[] msg);
}