package me.gregorias.kademlia.network;

import java.io.IOException;

/**
 * Interface for observers of byte array send related events.
 *
 * @author Grzegorz Milka
 */
public interface ByteResponseHandler {
  /**
   * Called when a message gets a response.
   *
   * @param response
   */
  void onResponse(byte[] response);

  /**
   * Called when a message couldn't get a response due to an error.
   *
   * @param exception
   */
  void onResponseError(IOException exception);

  /**
   * Called when a message has been successfully sent. This does not mean that a
   * message has been delivered.
   */
  void onSendSuccessful();

  /**
   * Called when a message could not be sent.
   *
   * @param exception
   */
  void onSendError(IOException exception);
}
