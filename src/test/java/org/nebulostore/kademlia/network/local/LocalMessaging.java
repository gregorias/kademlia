package org.nebulostore.kademlia.network.local;

import java.net.InetSocketAddress;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.nebulostore.kademlia.network.ByteListener;
import org.nebulostore.kademlia.network.ByteListeningService;
import org.nebulostore.kademlia.network.ByteResponseHandler;
import org.nebulostore.kademlia.network.ByteSender;
import org.nebulostore.kademlia.network.NetworkAddressDiscovery;

/**
 * Implementation of local messaging system.
 * 
 * @author Grzegorz Milka
 */
public class LocalMessaging {
	private static final NetworkAddressDiscovery NET_ADDR_DISCOVERY =
			new NetworkAddressDiscoveryImpl();
	private final ByteListeningService byteListeningService_;
	private final ByteSender byteSender_;
	private final ReadWriteLock rwLock_;
	private final Lock readLock_;
	private final Lock writeLock_;
	private ByteListener byteListener_;

	public LocalMessaging() {
		byteListeningService_ = new ByteListeningServiceImpl();
		byteSender_ = new ByteSenderImpl();
		rwLock_ = new ReentrantReadWriteLock();
		readLock_ = rwLock_.readLock();
		writeLock_ = rwLock_.writeLock();
	}
	
	public ByteListeningService getByteListeningService() {
		return byteListeningService_;
	}
	
	public ByteSender getByteSender() {
		return byteSender_;
	}
	
	public NetworkAddressDiscovery getNetworkAddressDiscovery() {
		return NET_ADDR_DISCOVERY;
	}

	private class ByteListeningServiceImpl implements ByteListeningService {
		@Override
		public void registerListener(ByteListener listener) {
			writeLock_.lock();
			try {
				if (byteListener_ != null) {
					throw new IllegalStateException("A listener is already registered.");
				}
				byteListener_ = listener;
			} finally {
				writeLock_.unlock();
			}
		}

		@Override
		public void unregisterListener(ByteListener listener) {
			writeLock_.lock();
			try {
				if (byteListener_ == null) {
					throw new IllegalStateException("No listener is registered.");
				}
				if (byteListener_ != listener) {
					throw new IllegalStateException("Given listener hasn't been registered.");
				}
				byteListener_ = null;
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
				if (byteListener_ == null) {
					throw new IllegalStateException("ByteListener hasn't been registered.");
				}
				response = byteListener_.receiveByteArrayWithResponse(array);
			} finally {
				readLock_.unlock();
			}
			handler.onSendSuccessful();
			handler.onResponse(response);
		}
	}
	
	private static class NetworkAddressDiscoveryImpl extends NetworkAddressDiscovery {
		private static final InetSocketAddress LOCAL_ADDRESS = new InetSocketAddress(0);

		@Override
		public InetSocketAddress getNetworkAddress() {
			return LOCAL_ADDRESS;
		}
	}
}
