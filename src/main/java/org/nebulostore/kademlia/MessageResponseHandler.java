package org.nebulostore.kademlia;

import java.io.IOException;

/**
 * Observer of message send and response events.
 * 
 * @author Grzegorz Milka
 */
interface MessageResponseHandler {
	void onResponse(Message response);
	void onResponseError(IOException e);
	void onSendSuccessful();
	void onSendError(IOException e);
}
