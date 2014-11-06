package me.gregorias.kademlia.core;

import java.io.IOException;

/**
 * Observer of message send and response events.
 *
 * @author Grzegorz Milka
 */
interface MessageResponseHandler {
  void onResponse(Message response);

  void onResponseError(IOException exception);

  void onSendSuccessful();

  void onSendError(IOException exception);
}
