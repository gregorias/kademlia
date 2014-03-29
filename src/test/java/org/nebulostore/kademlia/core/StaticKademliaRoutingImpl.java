package org.nebulostore.kademlia.core;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class StaticKademliaRoutingImpl implements KademliaRouting {
	private static final Logger LOGGER = LoggerFactory.getLogger(KademliaRoutingImpl.class);

	private final List<NodeInfo> bucket_;

	private final Key localKey_;
	private InetSocketAddress localAddress_;
	private final MessageSender messageSender_;
	private final ListeningService listeningService_;
	private final MessageListener messageListener_;
	private final int k_;

	private final Lock readRunningLock_;
	private final Lock writeRunningLock_;
	
	private boolean isRunning_ = false;

	private MessageListener auxListener_;
	
	StaticKademliaRoutingImpl(NodeInfo localNodeInfo, MessageSender sender,
			ListeningService listeningService, int k,
			Collection<NodeInfo> knownPeers) {
		assert k > 0;
		k_ = k;
		bucket_ = new LinkedList<>(knownPeers);
		localKey_ = localNodeInfo.getKey();
		localAddress_ = localNodeInfo.getSocketAddress();
		messageSender_ = sender;
		listeningService_ = listeningService;
		messageListener_ = new MessageListenerImpl();
		
		ReadWriteLock rwLock = new ReentrantReadWriteLock();
		readRunningLock_ = rwLock.readLock();
		writeRunningLock_ = rwLock.writeLock();
	}
	

	@Override
	public Collection<NodeInfo> findClosestNodes(Key key) {
		return findClosestNodes(key, 0);
	}

	@Override
	public Collection<NodeInfo> findClosestNodes(Key key, int i) {
		throw new UnsupportedOperationException("findClosestNodes is unsupported for this"
				+ " fake implementation");
	}

	@Override
	public Key getLocalKey() {
		return localKey_;
	}
	
	public boolean isRunning() {
		readRunningLock_.lock();
		boolean isRunning = isRunning_;
		readRunningLock_.unlock();
		return isRunning;
	}

	@Override
	public Collection<NodeInfo> getRoutingTable() {
		Collection<NodeInfo> routingTable = new ArrayList<>(bucket_);
		return routingTable;
	}
	
	public void sendPingToNode(Key key) {
		sendPingToNode(key, new IdleMessageResponseHandler());
	}

	public void sendPingToNode(Key key, MessageResponseHandler handler) {
		List<Key> keys = new LinkedList<>();
		keys.add(key);
		sendPingToNodes(keys, handler);
	}

	public void sendPingToNodes(Collection<Key> keys) {
		sendPingToNodes(keys, new IdleMessageResponseHandler());
	}
	
	public void sendPingToNodes(Collection<Key> keys, MessageResponseHandler handler) {
		Collection<NodeInfo> nodeInfos = new LinkedList<>();
		for (Key key : keys) {
			int index = findIndexOfNodeInfo(key);
			if (index != -1) {
				nodeInfos.add(bucket_.get(index));
			} else {
				throw new IllegalArgumentException(String.format("Key: %s is not present in"
						+ " routing table.", key));
			}
		}
		sendPingsToNodes(nodeInfos, handler);
	}
	
	public synchronized void setMessageListenerAdditionalActions(MessageListener listener) {
		auxListener_ = listener;
	}
	
	@Override
	public void start() throws KademliaException {
		writeRunningLock_.lock();
		try {
			LOGGER.info("start()");
			if (isRunning_) {
				throw new IllegalStateException("Kademlia has already started.");
			}
				
			LOGGER.trace("startUp() -> registerListener");
			listeningService_.registerListener(messageListener_);
			isRunning_ = true;
		} finally {
			writeRunningLock_.unlock();
		}
	}
	
	@Override
	public void stop() throws KademliaException {
		writeRunningLock_.lock();
		try {
			LOGGER.info("stop()");
			if (!isRunning_) {
				throw new IllegalStateException("Kademlia is not running.");
			}
			listeningService_.unregisterListener(messageListener_);
			isRunning_ = false;
		} finally {
			writeRunningLock_.unlock();
		}
		LOGGER.info("stop(): void");
	}

	/**
	 * Comparator of BitSets representing a 160-bit number in a little-endian encoding.
	 * 
	 * @author Grzegorz Milka
	 */
	private static class BitSetComparator implements Comparator<BitSet>, Serializable {
		private static final long serialVersionUID = 1L;
		public static BitSetComparator COMPARATOR = new BitSetComparator();

		@Override
		public int compare(BitSet b1, BitSet b2) { 
			int lengthComparison = Integer.compare(b1.length(), b2.length());
			if (lengthComparison != 0) {
				return lengthComparison;
			}
			
			int i = b1.length();
			while (i > 0 && b1.previousSetBit(i - 1) == b2.previousSetBit(i - 1)) {
				i = b1.previousSetBit(i - 1);
			}
			if (i == 0 || i == -1) {
				return 0;
			} else {
				assert i > 0;
				return Integer.compare(b1.previousSetBit(i - 1), b2.previousSetBit(i - 1));
			}
		}
	}
	
	private static class BoundedSortedSet<E> extends AbstractSet<E> implements SortedSet<E> {
		private final SortedSet<E> base_;
		private final int upperCountBound_;
		
		public BoundedSortedSet(SortedSet<E> base, int upperCountBound) {
			base_ = base;
			upperCountBound_ = upperCountBound;
		}
		
		@Override
		public boolean add(E element) {
			boolean hasBeenAdded = base_.add(element);
			if (!hasBeenAdded) {
				return hasBeenAdded;
			}
			
			if (base_.size() > upperCountBound_) {
				assert base_.size() == upperCountBound_ + 1;
				E lastElement_ = base_.last();
				base_.remove(lastElement_);
				return base_.comparator().compare(element, lastElement_) == 0;
			} else {
				return true;
			}
		}

		@Override
		public Comparator<? super E> comparator() {
			return base_.comparator();
		}

		@Override
		public E first() {
			return base_.first();
		}

		@Override
		public SortedSet<E> headSet(E toElement) {
			return base_.headSet(toElement);
		}

		@Override
		public Iterator<E> iterator() {
			return base_.iterator();
		}

		@Override
		public E last() {
			return base_.last();
		}

		@Override
		public int size() {
			return base_.size();
		}

		@Override
		public SortedSet<E> subSet(E fromElement, E toElement) {
			return base_.subSet(fromElement, toElement);
		}

		@Override
		public SortedSet<E> tailSet(E fromElement) {
			return base_.tailSet(fromElement);
		}
	}

	/**
	 * Comparator of Kademlia {@link Key} with specified key based on closeness.
	 * 
	 * @author Grzegorz Milka
	 */
	private static class KeyComparator implements Comparator<Key>, Serializable {
		private static final long serialVersionUID = 1L;
		private final Key referenceKey_;
		
		public KeyComparator(Key key) {
			referenceKey_ = key;
		}

		@Override
		public int compare(Key arg0, Key arg1) {
			BitSet distance0 = referenceKey_.calculateDistance(arg0);
			BitSet distance1 = referenceKey_.calculateDistance(arg1);

			return BitSetComparator.COMPARATOR.compare(distance0, distance1);
		}
	}

	
	private class MessageListenerImpl implements MessageListener {
		@Override
		public FindNodeReplyMessage receiveFindNodeMessage(FindNodeMessage msg) {
			synchronized (StaticKademliaRoutingImpl.this) {
				if (auxListener_ != null) {
					auxListener_.receiveFindNodeMessage(msg);
				}
			}
			return new FindNodeReplyMessage(getLocalNodeInfo(), msg.getSourceNodeInfo(), 
					getClosestRoutingTableNodes(msg.getSearchedKey(), k_));
		}

		@Override
		public PongMessage receiveGetKeyMessage(GetKeyMessage msg) {
			synchronized (StaticKademliaRoutingImpl.this) {
				if (auxListener_ != null) {
					auxListener_.receiveGetKeyMessage(msg);
				}
			}
			return new PongMessage(getLocalNodeInfo(), msg.getSourceNodeInfo());
		}

		@Override
		public PongMessage receivePingMessage(PingMessage msg) {
			synchronized (StaticKademliaRoutingImpl.this) {
				if (auxListener_ != null) {
					auxListener_.receivePingMessage(msg);
				}
			}
			return new PongMessage(getLocalNodeInfo(), msg.getSourceNodeInfo());
		}
	}

	private int findIndexOfNodeInfo(Key key) {
		int i = 0;
		synchronized (bucket_) {
			for (NodeInfo nodeInfo: bucket_) {
				if (nodeInfo.getKey().equals(key)) {
					return i;
				}
				++i;
			}
		}
		return -1;
	}


	private Collection<NodeInfo> getClosestRoutingTableNodes(final Key key, int size) {
		KeyComparator keyComparator = new KeyComparator(key);
		SortedSet<Key> closestKeys = new BoundedSortedSet<>(new TreeSet<>(keyComparator), size);
		Map<Key, NodeInfo> keyInfoMap_ = new HashMap<>();
		
		synchronized(bucket_) {
			for (NodeInfo nodeInfo: bucket_) {
				keyInfoMap_.put(nodeInfo.getKey(), nodeInfo);
				closestKeys.add(nodeInfo.getKey());
			}
		}
		keyInfoMap_.put(localKey_, getLocalNodeInfo());
		closestKeys.add(localKey_);
		
		Collection<NodeInfo> closestNodes = new ArrayList<>();
		for (Key closeKey: closestKeys) {
			closestNodes.add(keyInfoMap_.get(closeKey));
		}
		LOGGER.trace("getClosestRoutingTableNodes({}, {}): {}", key, size, closestNodes);
		return closestNodes;
	}
	
	private NodeInfo getLocalNodeInfo() {
		synchronized (localKey_) {
			return new NodeInfo(localKey_, localAddress_);
		}
	}

	private void sendPingsToNodes(Collection<NodeInfo> routingTableNodes, MessageResponseHandler handler) {
		for (NodeInfo info: routingTableNodes) {
			messageSender_.sendMessageWithReply(info.getSocketAddress(), new PingMessage(
					getLocalNodeInfo(), info), handler);
		}
	}
}
