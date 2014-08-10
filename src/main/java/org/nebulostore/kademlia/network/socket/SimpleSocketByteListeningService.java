package org.nebulostore.kademlia.network.socket;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
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
				byte[] msg = SimpleSocketByteSender.readMessage(clientSocket.getInputStream());
				if (msg == null) {
			    LOGGER.warn("Could not get full message from remote host.");
			    continue;
				}
				byte[] response = null;
				synchronized (this) {
					if (listener_ != null) {
						response = listener_.receiveByteArrayWithResponse(msg);
						if (response != null) {
						  byte[] intBuffer = SimpleSocketByteSender.transformIntToByteArray(response.length);
              clientSocket.getOutputStream().write(intBuffer);
              clientSocket.getOutputStream().write(response);
						}
						clientSocket.shutdownOutput();
					}
				}
			} catch (IOException e) {
				LOGGER.warn("Caught IOException during message exchange.", e);
			} finally {
				try {
					clientSocket.close();
				} catch (IOException e) {
          LOGGER.warn("Caught IOException when closing socket.", e);
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
}
