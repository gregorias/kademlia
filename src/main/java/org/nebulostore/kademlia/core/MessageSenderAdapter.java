package org.nebulostore.kademlia.core;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.nebulostore.kademlia.network.ByteResponseHandler;
import org.nebulostore.kademlia.network.ByteSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Adapter from {@link ByteSender} to {@link MessageSender}.
 * 
 * @author Grzegorz Milka
 */
class MessageSenderAdapter implements MessageSender {
	private static final Logger LOGGER = LoggerFactory.getLogger(MessageSenderAdapter.class);
	private ByteSender byteSender_;
	
	public MessageSenderAdapter(ByteSender byteSender) {
		byteSender_ = byteSender;
	}

	@Override
	public void sendMessageWithReply(InetSocketAddress dest, Message msg,
			MessageResponseHandler handler) {
		LOGGER.debug("sendMessageWithReply({}, {}, {})", dest, msg, handler);
		byte[] array = MessageSerializer.translateFromMessageToByte(msg);
		byteSender_.sendMessageWithReply(dest, array, new ByteResponseHandlerAdapter(handler));
	}
	
	private static class ByteResponseHandlerAdapter implements ByteResponseHandler {
		private final MessageResponseHandler handler_;

		public ByteResponseHandlerAdapter(MessageResponseHandler handler) {
			handler_ = handler;
		}

		@Override
		public void onResponse(byte[] response) {
			Message message = MessageSerializer.translateFromByteToMessage(response);
			if (message == null) {
				handler_.onResponseError(new IOException(
						"Could not deserialize response to correct message."));
			} else {
				handler_.onResponse(message);
			}
		}

		@Override
		public void onResponseError(IOException e) {
			handler_.onResponseError(e);
		}

		@Override
		public void onSendSuccessful() {
			handler_.onSendSuccessful();
		}

		@Override
		public void onSendError(IOException e) {
			handler_.onSendError(e);
		}
	}
}