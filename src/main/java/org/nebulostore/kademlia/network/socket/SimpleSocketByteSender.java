package org.nebulostore.kademlia.network.socket;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.LinkedList;
import java.util.List;

import org.nebulostore.kademlia.network.ByteResponseHandler;
import org.nebulostore.kademlia.network.ByteSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleSocketByteSender implements ByteSender {
	private static final Logger LOGGER = LoggerFactory.getLogger(SimpleSocketByteSender.class);
	private static final int BUFFER_LENGTH = 4096;

	public SimpleSocketByteSender() {
	}

	@Override
	public void sendMessageWithReply(InetSocketAddress dest, byte[] array,
			ByteResponseHandler handler) {
      LOGGER.debug("sendMessageWithReply({}, {}, {})", dest, array.length, handler);

      byte[] answer = null;
      boolean hasSent = false;
      try (Socket socket = new Socket(dest.getAddress(), dest.getPort())) {
    	  socket.getOutputStream().write(array);
    	  socket.shutdownOutput();
    	  hasSent = true;
    	  LOGGER.debug("sendMessageWithReply() -> sent successfully array of length: {}", array.length);
    	  handler.onSendSuccessful();

    	  answer = readTillClosed(socket.getInputStream());
      } catch (IOException e) {
    	  if (hasSent) {
    		  handler.onResponseError(e);
    	  } else {
    		  handler.onSendError(e);
    	  }
    	  return;
      }
	  LOGGER.debug("sendMessageWithReply() -> handler.onResponse(length: {})", answer.length);
      handler.onResponse(answer);
	}
	
	private byte[] readTillClosed(InputStream is) throws IOException {
		List<Byte> byteList = new LinkedList<>();
		byte[] buffer = new byte[BUFFER_LENGTH];
		while (true) {
			int len = is.read(buffer);
			if (len == -1) {
				break;
			}
			for (int i = 0; i < len; ++i) {
				byteList.add(buffer[i]);
			}
		}
		byte[] readBuffer = new byte[byteList.size()];
		int i = 0;
		for (Byte oneByte: byteList) {
			readBuffer[i] = oneByte;
			++i;
		}
		
		return readBuffer;
	}

}
