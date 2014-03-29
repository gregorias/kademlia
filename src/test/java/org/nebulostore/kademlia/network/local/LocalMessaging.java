package org.nebulostore.kademlia.network.local;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.nebulostore.kademlia.network.ByteListener;
import org.nebulostore.kademlia.network.ByteListeningService;
import org.nebulostore.kademlia.network.ByteResponseHandler;
import org.nebulostore.kademlia.network.ByteSender;
import org.nebulostore.kademlia.network.NetworkAddressDiscovery;
import org.nebulostore.kademlia.network.UserGivenNetworkAddressDiscovery;

/**
 * Implementation of local messaging system.
 * 
 * @author Grzegorz Milka
 */
public class LocalMessaging {
	private final ByteSender byteSender_;
	private final ReadWriteLock rwLock_;
	private final Lock readLock_;
	private final Lock writeLock_;
	private final Map<Integer, ByteListener> portToListenerMap_;

	public LocalMessaging() {
		byteSender_ = new ByteSenderImpl();
		rwLock_ = new ReentrantReadWriteLock();
		readLock_ = rwLock_.readLock();
		writeLock_ = rwLock_.writeLock();
		portToListenerMap_ = new HashMap<Integer, ByteListener>();
	}
	
	public ByteListeningService getByteListeningService(int port) {
		return new ByteListeningServiceImpl(port);
	}
	
	public ByteSender getByteSender() {
		return byteSender_;
	}
	
	public NetworkAddressDiscovery getNetworkAddressDiscovery(int port) {
		return new UserGivenNetworkAddressDiscovery(new InetSocketAddress(port));
	}

	private class ByteListeningServiceImpl implements ByteListeningService {
		private final int port_;
		
		public ByteListeningServiceImpl(int port) {
			port_ = port;
		}

		@Override
		public void registerListener(ByteListener listener) {
			writeLock_.lock();
			try {
				if (portToListenerMap_.containsKey(port_)) {
					throw new IllegalStateException(String.format("Port: %d already has a listener",
							port_));
				}
				portToListenerMap_.put(port_, listener);
			} finally {
				writeLock_.unlock();
			}
		}

		@Override
		public void unregisterListener(ByteListener listener) {
			writeLock_.lock();
			try {
				ByteListener byteListener = portToListenerMap_.get(port_);
				if (byteListener == null) {
					throw new IllegalStateException(String.format("Port: %d has no listener", port_));
				} else if (byteListener != listener) {
					throw new IllegalStateException("Given listener hasn't been registered.");
				}
				portToListenerMap_.remove(port_);
			} finally {
				writeLock_.unlock();
			}
		}
	}
	
	private class ByteSenderImpl implements ByteSender {
		@Override
		public void sendMessageWithReply(InetSocketAddress dest, byte[] array,
				ByteResponseHandler handler) {
			byte[] response;
			readLock_.lock();
			try {
				ByteListener byteListener = portToListenerMap_.get(dest.getPort());
				if (byteListener == null) {
					handler.onSendError(new IOException(String.format("ByteListener to port: %d hasn't"
							+ " been registered.", dest.getPort())));
					return;
				} else {
					response = byteListener.receiveByteArrayWithResponse(array);
				}
			} finally {
				readLock_.unlock();
			}
			handler.onSendSuccessful();
			if (response != null) {
				handler.onResponse(response);
			} else {
				handler.onResponseError(new IOException("Received null response"));
			}
		}
	}
}
