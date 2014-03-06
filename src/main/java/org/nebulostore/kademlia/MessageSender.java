package org.nebulostore.kademlia;

import java.net.InetSocketAddress;

interface MessageSender {
	void sendMessageWithReply(InetSocketAddress dest, Message msg, MessageResponseHandler handler);
}
