package org.nebulostore.kademlia.network.socket;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.nebulostore.kademlia.network.ByteListener;
import org.nebulostore.kademlia.network.ByteListeningService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link ByteListeningService} which creates a ServerSocket and
 * listens on single thread.
 * 
 * @author Grzegorz Milka
 */
public final class SimpleSocketByteListeningService implements ByteListeningService, Runnable {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(SimpleSocketByteListeningService.class);
	private static final int BUFFER_LENGTH = 4096;

	private final int port_;
	private final ExecutorService serviceExecutor_;

	private ServerSocket serverSocket_;
	private ByteListener listener_;

	public SimpleSocketByteListeningService(int port, ExecutorService executor) {
		port_ = port;
		serviceExecutor_ = executor;
	}

	@Override
	public synchronized void registerListener(ByteListener listener) {
		assert listener_ == null;
		listener_ = listener;
	}

	@Override
	public synchronized void unregisterListener(ByteListener listener) {
		assert listener_ == listener;
		listener_ = null;
	}

	@Override
	public void run() {
		while (!serverSocket_.isClosed()) {
			Socket clientSocket = null;
			try {
				clientSocket = serverSocket_.accept();
			} catch (IOException e) {
				if (serverSocket_.isClosed()) {
					LOGGER.trace("IOException when accepting connection. Socket is closed.", e);
					break;
				} else {
					LOGGER.warn("IOException when accepting connection. Socket is open.", e);
					continue;
				}
			}
			LOGGER.trace("Accepted connection from: " + clientSocket.getRemoteSocketAddress());

			try {
				byte[] msg = readTillClosed(clientSocket.getInputStream());
				byte[] response = null;
				synchronized (this) {
					if (listener_ != null) {
						response = listener_.receiveByteArrayWithResponse(msg);
						clientSocket.getOutputStream().write(response);
						clientSocket.shutdownOutput();
					}
				}
			} catch (IOException e) {
				LOGGER.warn("Caught IOException during message exchange.", e);
			} finally {
				try {
					clientSocket.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		LOGGER.trace("run(): void");
	}

	public void start() throws IOException {
		LOGGER.info("start() -> port: {}", port_);
		serverSocket_ = new ServerSocket(port_);
		serverSocket_.setReuseAddress(true);
		serviceExecutor_.execute(this);
	}

	public void stop() {
		LOGGER.info("stop()");
		try {
			serverSocket_.close();
		} catch (IOException e) {
			LOGGER.warn("IOException when closing server socket.", e);
		}
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
		for (Byte oneByte : byteList) {
			readBuffer[i] = oneByte;
			++i;
		}

		return readBuffer;
	}
}
