package org.nebulostore.kademlia.core;

interface MessageListener {
	/**
	 * Method to be called when a find node message is received.
	 * 
	 * @return {@link FindNodeReplyMessage} to be returned to sender or null if error has happened.
	 */
	FindNodeReplyMessage receiveFindNodeMessage(FindNodeMessage msg);

	/**
	 * Method to be called when a ping message is received.
	 * 
	 * @return {@link PongMessage} to be returned to sender or null if error has happened.
	 */
	PongMessage receivePingMessage(PingMessage msg);

	/**
	 * Method to be called when a get key message is received.
	 * 
	 * @return {@link PongMessage} to be returned to sender or null if error has happened.
	 */
	PongMessage receiveGetKeyMessage(GetKeyMessage msg);
}
