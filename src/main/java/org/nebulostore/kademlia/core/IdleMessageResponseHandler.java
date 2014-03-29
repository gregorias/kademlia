package org.nebulostore.kademlia.core;

import java.io.IOException;

/**
 * {@link MessageResponseHandler} which does nothing.
 * 
 * @author Grzegorz Milka
 */
class IdleMessageResponseHandler implements MessageResponseHandler {

	@Override
	public void onResponse(Message response) {
	}

	@Override
	public void onResponseError(IOException e) {
	}

	@Override
	public void onSendSuccessful() {
	}

	@Override
	public void onSendError(IOException e) {
	}
}