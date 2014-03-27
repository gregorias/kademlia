package org.nebulostore.kademlia.core;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.nebulostore.kademlia.network.ByteListener;
import org.nebulostore.kademlia.network.ByteListeningService;
import org.nebulostore.kademlia.network.ByteResponseHandler;
import org.nebulostore.kademlia.network.ByteSender;
import org.nebulostore.kademlia.network.NetworkAddressDiscovery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builder of Kademlia routing peers.
 * 
 * If you want multiple kademlia peers on the same listening connection you have
 * to: 1. Set {@link ByteListener} only once 2. Use the same builder for all
 * peers.
 * 
 * This class is not thread safe.
 * 
 * @author Grzegorz Milka
 */
public class KademliaRoutingBuilder {
	private static final Logger LOGGER = LoggerFactory.getLogger(KademliaRoutingBuilder.class);
	private final Random random_;
	private static final int DEFAULT_ENTRY_REFRESH_DELAY = 60 * 60 * 1000;
	private static final int DEFAULT_K = 10;
	private static final int DEFAULT_ALPHA = 5;

	private ListeningService listeningAdapter_;
	private DemultiplexingMessageListener demultiplexingListener_;

	private MessageSenderAdapter messageSender_;
	private NetworkAddressDiscovery netAddrDiscovery_;
	private ScheduledExecutorService executor_;
	private int k_ = DEFAULT_K;
	private int alpha_ = DEFAULT_ALPHA;
	private Key key_;
	private Collection<InetSocketAddress> initialPeersWithoutKeys_ = new LinkedList<>();
	private Collection<NodeInfo> initialPeersWithKeys_ = new LinkedList<>();
	private int entryRefreshDelay_ = DEFAULT_ENTRY_REFRESH_DELAY;
	
	public KademliaRoutingBuilder(Random random) {
		random_ = random;
	}
	
	/**
	 * Create inactive Kademlia peer.
	 * 
	 * @return Kademlia peer with parameters set before.
	 * @throws IllegalStateException
	 */
	public KademliaRouting createPeer() throws IllegalStateException {
		LOGGER.info("createPeer()");

		checkIfByteListeningServiceIsSet();
		checkIfByteSenderIsSet();
		checkIfExecutorIsSet();
		checkIfNetAddrDiscoveryIsSet();

		Key usedKey = getSetKeyOrCreateNew();
		NodeInfo localNodeInfo = new NodeInfo(usedKey, netAddrDiscovery_.getNetworkAddress());

		ListeningService listeningService = new MessageListeningServiceImpl(usedKey,
				demultiplexingListener_);
		LOGGER.debug("createPeer() -> Key: {}", usedKey);
		return new KademliaRoutingImpl(localNodeInfo, messageSender_, listeningService,
				netAddrDiscovery_, executor_, k_, alpha_, entryRefreshDelay_,
				initialPeersWithoutKeys_, initialPeersWithKeys_, random_);
	}

	/**
	 * Set size of one bucket.
	 * @param k size of bucket
	 * @return this
	 */
	public KademliaRoutingBuilder setBucketSize(int k) {
		k_ = k;
		return this;
	}

	/**
	 * Set {@link ByteSender} used for sending messages.
	 * 
	 * ByteSender may be shared between multiple instances of kademlia.
	 * 
	 * @param byteSender ByteSender used for sending message
	 * @return this
	 */
	public KademliaRoutingBuilder setByteSender(ByteSender byteSender) {
		messageSender_ = new MessageSenderAdapter(byteSender);
		return this;
	}

	/**
	 * Set {@link ByteListeningService}.
	 * 
	 * ByteListeningService may be shared between multiple instances of kademlia,
	 * but they must created from the same builder.
	 * 
	 * @param bLS
	 * @return this
	 */
	public KademliaRoutingBuilder setByteListeningService(ByteListeningService bLS) {
		listeningAdapter_ = new MessageListeningServiceAdapter(bLS);
		demultiplexingListener_ = new DemultiplexingMessageListener(listeningAdapter_);
		return this;
	}

	/**
	 * @param alpha Concurrency parameter used for sending
	 * @return this
	 */
	public KademliaRoutingBuilder setConcurrencyParameter(int alpha) {
		alpha_ = alpha;
		return this;
	}
	
	/**
	 * Set delay used for periodic refreshing local node in the network.
	 * 
	 * @param delay delay in miliseconds
	 * @return this
	 */
	public KademliaRoutingBuilder setEntryRefreshingDelay(int delay) {
		entryRefreshDelay_ = delay;
		return this;
	}

	public KademliaRoutingBuilder setExecutor(ScheduledExecutorService executor) {
		executor_ = executor;
		return this;
	}

	/**
	 * Set initial peers connected to the network, but of unknown kademlia keys.
	 * 
	 * @param peerAddresses
	 * @return this
	 */
	public KademliaRoutingBuilder setInitialPeersWithoutKeys(Collection<InetSocketAddress> peerAddresses) {
		assert peerAddresses != null;
		initialPeersWithoutKeys_ = new LinkedList<>(peerAddresses);
		return this;
	}

	/**
	 * Set initial peers connected to the network with known kademlia keys.
	 * 
	 * @param peerInfos
	 * @return this
	 */
	public KademliaRoutingBuilder setInitialPeersWithKeys(Collection<NodeInfo> peerInfos) {
		assert peerInfos != null;
		initialPeersWithKeys_ = new LinkedList<>(peerInfos);
		return this;
	}

	public KademliaRoutingBuilder setKey(Key key) {
		key_ = key;
		return this;
	}

	/**
	 * @param netAddrDisc
	 * @return this
	 */
	public KademliaRoutingBuilder setNetworkAddressDiscovery(NetworkAddressDiscovery netAddrDisc) {
		netAddrDiscovery_ = netAddrDisc;
		return this;
	}

	private static class DemultiplexingMessageListener implements MessageListener {
		private final ListeningService baseListeningService_;
		private final Map<Key, MessageListener> listenerMap_;
		private final ReadWriteLock rwLock_;
		private final Lock readLock_;
		private final Lock writeLock_;

		public DemultiplexingMessageListener(ListeningService baseListeningService) {
			baseListeningService_ = baseListeningService;
			listenerMap_ = new HashMap<>();
			rwLock_ = new ReentrantReadWriteLock();
			readLock_ = rwLock_.readLock();
			writeLock_ = rwLock_.writeLock();
		}

		public void registerListener(Key key, MessageListener listener) {
			writeLock_.lock();
			try {
				if (listenerMap_.isEmpty()) {
					baseListeningService_.registerListener(this);
				}
				if (listenerMap_.containsKey(key)) {
					throw new IllegalStateException(String.format("Kademlia peer at key: %s"
							+ " has already registered its listener.", key));
				}
				listenerMap_.put(key, listener);
			} finally {
				writeLock_.unlock();
			}
		}

		public void unregisterListener(Key key) {
			writeLock_.lock();
			try {
				if (!listenerMap_.containsKey(key)) {
					throw new IllegalStateException(String.format("Kademlia peer at key: %s"
							+ " has no registered listener.", key));
				}
				listenerMap_.remove(key);
				if (listenerMap_.isEmpty()) {
					baseListeningService_.unregisterListener(this);
				}
			} finally {
				writeLock_.unlock();
			}
		}

		@Override
		public FindNodeReplyMessage receiveFindNodeMessage(FindNodeMessage msg) {
			readLock_.lock();
			try {
				MessageListener listener = getRecipient(msg);
				if (listener == null) {
					return null;
				}
				return listener.receiveFindNodeMessage(msg);
			} finally {
				readLock_.unlock();
			}
		}

		@Override
		public PongMessage receivePingMessage(PingMessage msg) {
			readLock_.lock();
			try {
				MessageListener listener = getRecipient(msg);
				if (listener == null) {
					return null;
				}
				return listener.receivePingMessage(msg);
			} finally {
				readLock_.unlock();
			}
		}

		@Override
		public PongMessage receiveGetKeyMessage(GetKeyMessage msg) {
			readLock_.lock();
			try {
				if (listenerMap_.isEmpty()) {
					LOGGER.warn("receiveGetKeyMessage({}) -> no listener is registered.", msg);
				}
				MessageListener listener = listenerMap_.values().iterator().next();
				return listener.receiveGetKeyMessage(msg);
			} finally {
				readLock_.unlock();
			}
		}

		private MessageListener getRecipient(MessageWithKnownRecipient msg) {
			Key destKey = msg.getDestinationNodeInfo().getKey();
			MessageListener listener = listenerMap_.get(destKey);
			if (listener == null) {
				LOGGER.warn("getRecipient({}) -> Received message to unknown kademlia peer.", msg);
			}
			return listener;
		}

	}

	private static class MessageListeningServiceAdapter implements ListeningService {
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
				Message msg = translateFromByteToMessage(byteMsg);
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
					return translateFromMessageToByte(response);
				}
			}

		}
	}

	private static class MessageListeningServiceImpl implements ListeningService {
		private static final Logger LOGGER = LoggerFactory.getLogger(MessageListeningServiceImpl.class);
		private final DemultiplexingMessageListener demux_;
		private final Key key_;

		public MessageListeningServiceImpl(Key key, DemultiplexingMessageListener demux) {
			demux_ = demux;
			key_ = key;
		}

		@Override
		public void registerListener(MessageListener listener) {
			LOGGER.trace("registerListener({})", listener);
			demux_.registerListener(key_, listener);
		}

		@Override
		public void unregisterListener(MessageListener listener) {
			LOGGER.trace("unregisterListener({})", listener);
			demux_.unregisterListener(key_);
			LOGGER.trace("unregisterListener(): void");
		}
	}

	private static class MessageSenderAdapter implements MessageSender {
		private static final Logger LOGGER = LoggerFactory.getLogger(MessageSenderAdapter.class);
		private ByteSender byteSender_;
		
		public MessageSenderAdapter(ByteSender byteSender) {
			byteSender_ = byteSender;
		}

		@Override
		public void sendMessageWithReply(InetSocketAddress dest, Message msg,
				MessageResponseHandler handler) {
			LOGGER.debug("sendMessageWithReply({}, {}, {})", dest, msg, handler);
			byte[] array = translateFromMessageToByte(msg);
			byteSender_.sendMessageWithReply(dest, array, new ByteResponseHandlerAdapter(handler));
		}
		
		private static class ByteResponseHandlerAdapter implements ByteResponseHandler {
			private final MessageResponseHandler handler_;

			public ByteResponseHandlerAdapter(MessageResponseHandler handler) {
				handler_ = handler;
			}

			@Override
			public void onResponse(byte[] response) {
				Message message = translateFromByteToMessage(response);
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

	private void checkIfByteListeningServiceIsSet() {
		if (listeningAdapter_ == null) {
			throw new IllegalStateException("Listening service is not set.");
		}
	}

	private void checkIfByteSenderIsSet() {
		if (messageSender_ == null) {
			throw new IllegalStateException("Byte sender is not set.");
		}
	}

	private void checkIfExecutorIsSet() {
		if (executor_ == null) {
			throw new IllegalStateException("Executor is not set.");
		}
	}

	private void checkIfNetAddrDiscoveryIsSet() {
		if (netAddrDiscovery_ == null) {
			throw new IllegalStateException("Network address discovery is not set.");
		}
	}

	private Key getSetKeyOrCreateNew() {
		if (key_ != null) {
			return key_;
		} else {
			return Key.newRandomKey(random_);
		}
	}

	private static Message translateFromByteToMessage(byte[] byteMsg) {
		Message msg = null;
		InputStream bais = new ByteArrayInputStream(byteMsg);
		ObjectInputStream ois = null;
		try {
			ois = new ObjectInputStream(bais);
			msg = (Message) ois.readObject();
		} catch (ClassCastException | ClassNotFoundException e) {
			LOGGER.warn("translateFromByteToMessage() -> Could not deserialize message", e);
		} catch (IOException e) {
			LOGGER.error("translateFromByteToMessage() -> Caught unexpected IOException", e);
		} finally {
			try {
				if (ois != null) {
					ois.close();
				}
			} catch (IOException e) {
				LOGGER.error("translateFromByteToMessage() -> Caught unexpected"
						+ " IOException during closing of stream", e);
			} finally {
				try {
					bais.close();
				} catch (IOException e) {
					LOGGER.error("translateFromByteToMessage() -> Caught unexpected"
							+ " IOException during closing of byte stream", e);
				}
			}
		}
		return msg;
	}

	private static byte[] translateFromMessageToByte(Message message) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = null;
		try {
			oos = new ObjectOutputStream(baos);
			oos.writeObject(message);
		} catch (IOException e) {
			LOGGER.error("translateFromMessageToByte() -> Caught unexpected IOException", e);
			return null;
		} finally {
			try {
				if (oos != null) {
					oos.close();
				}
			} catch (IOException e) {
				LOGGER.error("translateFromMessageToByte() -> Caught unexpected"
						+ " IOException during closing of stream", e);
				return null;
			} finally {
				try {
					baos.close();
				} catch (IOException e) {
					LOGGER.error("translateFromMessageToByte() -> Caught unexpected"
							+ " IOException during closing of byte stream", e);
					return null;
				}
			}
		}
		return baos.toByteArray();
	}
}
