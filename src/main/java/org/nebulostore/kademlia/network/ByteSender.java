package org.nebulostore.kademlia.network;

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
	 * @param dest
	 * @param array
	 * @param handler
	 */
	void sendMessageWithReply(InetSocketAddress dest, byte[] array, ByteResponseHandler handler);
}