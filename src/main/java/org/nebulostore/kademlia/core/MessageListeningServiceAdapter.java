package org.nebulostore.kademlia.core;

import org.nebulostore.kademlia.network.ByteListener;
import org.nebulostore.kademlia.network.ByteListeningService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Adapter from {@link ByteListeningService} to {@link ListeningService}.
 * 
 * @author Grzegorz Milka
 */
class MessageListeningServiceAdapter implements ListeningService {
	private static final Logger LOGGER = LoggerFactory.getLogger(MessageListeningServiceAdapter.class);
	private final ByteListeningService byteListeningService_;
	private final ByteToMessageTranslatingListener byteToMsgListener_;
	private MessageListener listener_;

	public MessageListeningServiceAdapter(ByteListeningService byteListeningService) {
		byteListeningService_ = byteListeningService;
		byteToMsgListener_ = new ByteToMessageTranslatingListener();
	}

	@Override
	public void registerListener(MessageListener listener) {
		assert listener_ == null;
		listener_ = listener;
		byteListeningService_.registerListener(byteToMsgListener_);

	}

	@Override
	public void unregisterListener(MessageListener listener) {
		assert listener_ != null && listener_.equals(listener);
		byteListeningService_.unregisterListener(byteToMsgListener_);
		listener_ = null;
	}

	private class ByteToMessageTranslatingListener implements ByteListener {
		@Override
		public byte[] receiveByteArrayWithResponse(byte[] byteMsg) {
			Message msg = MessageSerializer.translateFromByteToMessage(byteMsg);
			Message response = null;
			if (msg instanceof FindNodeMessage) {
				response = listener_.receiveFindNodeMessage((FindNodeMessage) msg);
			} else if (msg instanceof GetKeyMessage) {
				response = listener_.receiveGetKeyMessage((GetKeyMessage) msg);
			} else if (msg instanceof PingMessage) {
				response = listener_.receivePingMessage((PingMessage) msg);
			} else {
				LOGGER.error("receiveByteArrayWithResponse() -> received unexpected type");
			}
			if (response == null) {
				return null;
			} else {
				return MessageSerializer.translateFromMessageToByte(response);
			}
		}
	}
}