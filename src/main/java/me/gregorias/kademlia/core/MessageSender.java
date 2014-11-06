package me.gregorias.kademlia.core;

import java.net.InetSocketAddress;

interface MessageSender {
  void sendMessageWithReply(InetSocketAddress dest, Message msg, MessageResponseHandler handler);
}
