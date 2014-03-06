package org.nebulostore.kademlia.network;

import java.io.IOException;

/**
 * Interface for observers of byte array send related events.
 * 
 * @author Grzegorz Milka
 */
public interface ByteResponseHandler {
	void onResponse(byte[] response);
	void onResponseError(IOException e);
	void onSendSuccessful();
	void onSendError(IOException e);
}
