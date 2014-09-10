package org.nebulostore.kademlia.network.socket;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;

import org.nebulostore.kademlia.network.ByteResponseHandler;
import org.nebulostore.kademlia.network.ByteSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link ByteSender} which sends messages in a blocking way.
 *
 * @author Grzegorz Milka
 */
public class SimpleSocketByteSender implements ByteSender {
  public static final int INT_LENGTH = 4;
  private static final Logger LOGGER = LoggerFactory.getLogger(SimpleSocketByteSender.class);

  public SimpleSocketByteSender() {
  }

  /**
   * Send given message. This method blocks till the sending operation is
   * finished.
   */
  @Override
  public void sendMessageWithReply(InetSocketAddress dest,
      byte[] array,
      ByteResponseHandler handler) {
    LOGGER.debug("sendMessageWithReply({}, {}, {})", dest, array.length, handler);

    byte[] answer = null;
    boolean hasSent = false;
    try (Socket socket = new Socket(dest.getAddress(), dest.getPort())) {
      socket.getOutputStream().write(transformIntToByteArray(array.length));
      socket.getOutputStream().write(array);
      socket.shutdownOutput();
      hasSent = true;
      LOGGER.debug("sendMessageWithReply() -> sent successfully array of length: {}", array.length);
      handler.onSendSuccessful();

      answer = readMessage(socket.getInputStream());
    } catch (IOException e) {
      if (hasSent) {
        handler.onResponseError(e);
      } else {
        handler.onSendError(e);
      }
      return;
    }
    if (answer == null) {
      LOGGER.debug("sendMessageWithReply() -> handler.onResponseError()");
      handler.onResponseError(new EOFException("Could not get response message."));
    } else {
      LOGGER.debug("sendMessageWithReply() -> handler.onResponse(length: {})", answer.length);
      handler.onResponse(answer);
    }

  }

  static byte[] readBytes(InputStream is, int byteCnt) throws IOException {
    int remaining = byteCnt;
    byte[] array = new byte[byteCnt];
    while (remaining != 0) {
      int len = is.read(array, byteCnt - remaining, remaining);
      if (len < 1) {
        return null;
      }
      remaining -= len;
    }
    return array;
  }

  static byte[] readMessage(InputStream is) throws IOException {
    byte[] intArray = readBytes(is, INT_LENGTH);
    if (intArray == null) {
      return null;
    }
    IntBuffer intBuffer = ByteBuffer.wrap(intArray).asIntBuffer();
    int msgLength = intBuffer.get();

    byte[] msgArray = readBytes(is, msgLength);
    return msgArray;
  }

  static int transformByteArrayToInt(byte[] array) {
    return ByteBuffer.wrap(array).asIntBuffer().get();
  }

  static byte[] transformIntToByteArray(int length) {
    ByteBuffer byteBuffer = ByteBuffer.allocate(INT_LENGTH);
    byteBuffer.putInt(length);
    return byteBuffer.array();
  }
}
