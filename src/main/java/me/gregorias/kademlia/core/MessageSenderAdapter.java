package me.gregorias.kademlia.core;

import java.io.IOException;
import java.net.InetSocketAddress;

import me.gregorias.kademlia.network.ByteResponseHandler;
import me.gregorias.kademlia.network.ByteSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Adapter from {@link me.gregorias.kademlia.network.ByteSender} to {@link MessageSender}.
 *
 * @author Grzegorz Milka
 */
class MessageSenderAdapter implements MessageSender {
  private static final Logger LOGGER = LoggerFactory.getLogger(MessageSenderAdapter.class);
  private ByteSender mByteSender;

  public MessageSenderAdapter(ByteSender byteSender) {
    mByteSender = byteSender;
  }

  @Override
  public void sendMessageWithReply(InetSocketAddress dest, Message msg,
      MessageResponseHandler handler) {
    LOGGER.debug("sendMessageWithReply({}, {}, {})", dest, msg, handler);
    byte[] array = MessageSerializer.translateFromMessageToByte(msg);
    mByteSender.sendMessageWithReply(dest, array, new ByteResponseHandlerAdapter(handler));
  }

  private static class ByteResponseHandlerAdapter implements ByteResponseHandler {
    private final MessageResponseHandler mHandler;

    public ByteResponseHandlerAdapter(MessageResponseHandler handler) {
      mHandler = handler;
    }

    @Override
    public void onResponse(byte[] response) {
      Message message = MessageSerializer.translateFromByteToMessage(response);
      if (message == null) {
        mHandler.onResponseError(new IOException(
            "Could not deserialize response to correct message."));
      } else {
        mHandler.onResponse(message);
      }
    }

    @Override
    public void onResponseError(IOException exception) {
      mHandler.onResponseError(exception);
    }

    @Override
    public void onSendSuccessful() {
      mHandler.onSendSuccessful();
    }

    @Override
    public void onSendError(IOException exception) {
      mHandler.onSendError(exception);
    }
  }
}