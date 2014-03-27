package org.nebulostore.kademlia.core;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.nebulostore.kademlia.network.NetworkAddressDiscovery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.SettableFuture;

class KademliaRoutingImpl implements KademliaRouting {
	private static final Logger LOGGER = LoggerFactory.getLogger(KademliaRoutingImpl.class);
	private final Random random_;

	private final Collection<InetSocketAddress> initialPeerAddresses_;
	private final Collection<NodeInfo> initialKnownPeers_;

	private final List<List<NodeInfo>> kBuckets_;

	/**
	 * List of nodes expecting to be put into a bucket if a place for them is emptied.
	 */
	private final List<List<NodeInfo>> kBucketsWaitingList_;
	private final Key localKey_;
	private InetSocketAddress localAddress_;
	private final MessageSender messageSender_;
	private final ListeningService listeningService_;
	private final MessageListener messageListener_;
	private final NetworkAddressDiscovery netAddrDiscovery_;
	private final int k_;
	/**
	 * Concurrency coefficient in node lookups
	 */
	private final int alpha_;
	private final int entryRefreshDelay_;

	private final ScheduledExecutorService scheduledExecutor_;
	
	private final EntryRefresher entryRefresher_;
	private final AddressChangeObserver addrChangeObserver_;
	
	private final Lock readRunningLock_;
	private final Lock writeRunningLock_;
	
	private boolean isRunning_ = false;
	private ScheduledFuture<?> refreshFuture_ = null; 
	
	KademliaRoutingImpl(NodeInfo localNodeInfo, MessageSender sender,
			ListeningService listeningService, NetworkAddressDiscovery netAddrDiscovery,
			ScheduledExecutorService scheduledExecutor, int k, int alpha,
			int entryRefreshDelay,
			Collection<InetSocketAddress> initialPeerAddresses,
			Collection<NodeInfo> initialKnownPeers, Random random) {
		assert k > 0;
		random_ = random;
		k_ = k;
		alpha_ = alpha;
		entryRefreshDelay_ = entryRefreshDelay;
		kBuckets_ = initializeKBuckets();
		kBucketsWaitingList_ = initializeKBuckets();
		localKey_ = localNodeInfo.getKey();
		localAddress_ = localNodeInfo.getSocketAddress();
		messageSender_ = sender;
		listeningService_ = listeningService;
		messageListener_ = new MessageListenerImpl();
		netAddrDiscovery_ = netAddrDiscovery;
		
		scheduledExecutor_ = scheduledExecutor;
		
		initialPeerAddresses_ = new LinkedList<>(initialPeerAddresses);
		initialKnownPeers_ = new LinkedList<>(initialKnownPeers);
		
		entryRefresher_ = new EntryRefresher();
		addrChangeObserver_ = new AddressChangeObserver();
		
		ReadWriteLock rwLock = new ReentrantReadWriteLock();
		readRunningLock_ = rwLock.readLock();
		writeRunningLock_ = rwLock.writeLock();
	}
	

	@Override
	public Collection<NodeInfo> findClosestNodes(Key key) throws InterruptedException {
		return findClosestNodes(key, k_);
	}


	@Override
	public Collection<NodeInfo> findClosestNodes(Key key, int size) throws InterruptedException {
		final KeyComparator findKeyComparator = new KeyComparator(key);
		LOGGER.debug("findClosestKeys({}, {})", key.toString(), size);
		readRunningLock_.lock();
		try {
			if (!isRunning_) {
				throw new IllegalStateException("Called findClosestNodes() on nonrunning kademlia.");
			}
			Map<Key, NodeInfo> keyInfoMap = new HashMap<>();
			Set<Key> queriedKeys = new HashSet<>();
			PriorityQueue<Key> unqueriedKeys = new PriorityQueue<>(k_, findKeyComparator);
			SortedSet<Key> candidateKeys = new BoundedSortedSet<>(new TreeSet<>(findKeyComparator), size);
	
			BlockingQueue<Future<Message>> replyQueue = new LinkedBlockingQueue<>();
			MessageResponseHandler responseHandler = new FindNodesMessageResponseHandler(replyQueue);
	
			Collection<NodeInfo> closestNodes = getClosestRoutingTableNodes(key, Math.max(alpha_, k_));
	
			for (NodeInfo nodeInfo: closestNodes) {
				unqueriedKeys.add(nodeInfo.getKey());
				keyInfoMap.put(nodeInfo.getKey(), nodeInfo);
			}
	
			int i = 0;
			while (i < alpha_ && !unqueriedKeys.isEmpty()) {
				Key closestKey = unqueriedKeys.remove();
				NodeInfo nodeInfo = keyInfoMap.get(closestKey);
	
				queriedKeys.add(closestKey);
				messageSender_.sendMessageWithReply(nodeInfo.getSocketAddress(),
						new FindNodeMessage(getLocalNodeInfo(), nodeInfo, key),
						responseHandler);
				i += 1;
			}
			
			while (i > 0) {
				Future<Message> future = replyQueue.take();
				i -= 1;
				try {
					FindNodeReplyMessage msg = (FindNodeReplyMessage) future.get();
					for (NodeInfo foundNodeInfo: msg.getFoundNodes()) {
						NodeInfo nodeInfoInMap = keyInfoMap.get(foundNodeInfo.getKey());
						if (nodeInfoInMap != null && !nodeInfoInMap.getSocketAddress().equals(
								foundNodeInfo.getSocketAddress())) {
							/* Ignore */
						} else if (!queriedKeys.contains(foundNodeInfo.getKey())) {
							unqueriedKeys.add(foundNodeInfo.getKey());
							if (nodeInfoInMap == null) {
								keyInfoMap.put(foundNodeInfo.getKey(), foundNodeInfo);
							}
						}
					}
					
					/* add source key to candidates */
					Key sourceKey = msg.getSourceNodeInfo().getKey();
					LOGGER.trace("findClosestNodes() -> candidateKeys.add({})", sourceKey);
					candidateKeys.add(sourceKey);
	
					while (!unqueriedKeys.isEmpty()) {
						Key newKey = unqueriedKeys.remove();
						queriedKeys.add(newKey);
						if (candidateKeys.size() < size ||
								findKeyComparator.compare(newKey, candidateKeys.last()) < 0) {
							NodeInfo nodeInfo = keyInfoMap.get(newKey);
							messageSender_.sendMessageWithReply(nodeInfo.getSocketAddress(),
									new FindNodeMessage(getLocalNodeInfo(), nodeInfo, key),
									responseHandler);
							i += 1;
						} 
					}
				} catch (ExecutionException e) {
					LOGGER.debug(String.format(
							"findClosestKeys(%s) -> exception when sending a message.", key.toString()),
							e);
				} catch (ClassCastException e) {
					LOGGER.warn(String.format(
							"findClosestKeys(%s) -> received message of wrong type.", key.toString()),
							e);
				}
			}
			
			Collection<NodeInfo> closestFoundNodeInfos = new ArrayList<>();
			closestFoundNodeInfos.addAll(keyInfoMap.values());
			LOGGER.debug("findClosestKeys(): {}", closestFoundNodeInfos);
			return closestFoundNodeInfos;
		} finally {
			readRunningLock_.unlock();
		}
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
		Collection<NodeInfo> routingTable = new ArrayList<>();
		for (Collection<NodeInfo> bucket: kBuckets_) {
			routingTable.addAll(bucket);
		}
		return routingTable;
	}
	
	@Override
	public void start() throws KademliaException {
		writeRunningLock_.lock();
		try {
			LOGGER.info("start()");
			if (isRunning_) {
				throw new IllegalStateException("Kademlia has already started.");
			}
				
			/* Connect to initial peers */
			clearBuckets();
			LOGGER.trace("startUp() -> addPeersToBuckets");
			addPeersToBuckets(initialKnownPeers_);
			LOGGER.trace("startUp() -> checkKeysOfUnknownPeers");
			checkKeysOfUnknownPeers(initialPeerAddresses_);
			netAddrDiscovery_.addObserver(addrChangeObserver_);
			LOGGER.trace("startUp() -> registerListener");
			listeningService_.registerListener(messageListener_);
			refreshFuture_ = scheduledExecutor_.schedule(entryRefresher_, 0, TimeUnit.MILLISECONDS);
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
			refreshFuture_.cancel(true);
			listeningService_.unregisterListener(messageListener_);
			isRunning_ = false;
		} finally {
			writeRunningLock_.unlock();
		}
		LOGGER.info("stop(): void");
	}

	private class AddressChangeObserver implements Observer {
		@Override
		public void update(Observable arg0, Object arg1) {
			InetSocketAddress newAddress = (InetSocketAddress) arg1;
			changeAddress(newAddress);
			scheduledExecutor_.schedule(entryRefresher_, 0, TimeUnit.MILLISECONDS);
		}
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
	
	private class EntryRefresher implements Runnable {
		private final Logger LOGGER = LoggerFactory.getLogger(EntryRefresher.class);
		@Override
		public synchronized void run() {
			LOGGER.debug("run()");
			readRunningLock_.lock();
			try {
				if (!isRunning_) {
					return;
				}
				findClosestNodes(localKey_);
				refreshFuture_ = scheduledExecutor_.schedule(this, entryRefreshDelay_, TimeUnit.MILLISECONDS);
			} catch (InterruptedException e) {
				LOGGER.error("Caught unexpected InterruptedException.", e);
			} finally {
				readRunningLock_.unlock();
			}
			LOGGER.debug("run(): void");
		}
		
	}

	private class FindNodesMessageResponseHandler implements MessageResponseHandler {
		private final BlockingQueue<Future<Message>> outputQueue_;
		
		public FindNodesMessageResponseHandler(BlockingQueue<Future<Message>> outputQueue) {
			outputQueue_ = outputQueue;
		}

		@Override
		public void onResponse(Message response) {
			SettableFuture<Message> futureResponse = SettableFuture.<Message>create();
			futureResponse.set(response);
			processSenderNodeInfo(response.getSourceNodeInfo());
			outputQueue_.add(futureResponse);
		}

		@Override
		public void onResponseError(IOException e) {
			SettableFuture<Message> futureResponse = SettableFuture.<Message>create();
			futureResponse.setException(e);
			outputQueue_.add(futureResponse);
		}

		@Override
		public void onSendSuccessful() {
		}

		@Override
		public void onSendError(IOException e) {
			SettableFuture<Message> futureResponse = SettableFuture.<Message>create();
			futureResponse.setException(e);
			outputQueue_.add(futureResponse);
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
			processSenderNodeInfo(msg.getSourceNodeInfo());
			return new FindNodeReplyMessage(getLocalNodeInfo(), msg.getSourceNodeInfo(), 
					getClosestRoutingTableNodes(msg.getSearchedKey(), k_));
		}

		@Override
		public PongMessage receiveGetKeyMessage(GetKeyMessage msg) {
			processSenderNodeInfo(msg.getSourceNodeInfo());
			return new PongMessage(getLocalNodeInfo(), msg.getSourceNodeInfo());
		}

		@Override
		public PongMessage receivePingMessage(PingMessage msg) {
			processSenderNodeInfo(msg.getSourceNodeInfo());
			return new PongMessage(getLocalNodeInfo(), msg.getSourceNodeInfo());
		}
	}

	private class PingCheckMessageHandler implements MessageResponseHandler {
		private final int bucketId_;
		private final List<NodeInfo> bucket_;
		private final List<NodeInfo> bucketWaitingList_;
		
		public PingCheckMessageHandler(int bucketId) {
			bucketId_ = bucketId;
			bucket_ = kBuckets_.get(bucketId_);
			bucketWaitingList_ = kBucketsWaitingList_.get(bucketId_);
		}
		
		
		@Override
		public void onResponse(Message response) {
			LOGGER.debug("onResponse()");
			synchronized (bucket_) {
				replaceNodeInfo(bucket_, 0, bucket_.get(0));
				bucketWaitingList_.remove(0);
				if (!bucketWaitingList_.isEmpty()) {
					messageSender_.sendMessageWithReply(bucketWaitingList_.get(0).getSocketAddress(),
							new PingMessage(getLocalNodeInfo(), bucketWaitingList_.get(0)), 
							this);
				}
			}
		}

		@Override
		public void onResponseError(IOException e) {
			LOGGER.debug("onResponseError()", e);
			onError();
		}

		@Override
		public void onSendSuccessful() {
		}

		@Override
		public void onSendError(IOException e) {
			LOGGER.debug("onSendError()", e);
			onError();
		}


		private void onError() {
			synchronized (bucket_) {
				replaceNodeInfo(bucket_, 0, bucketWaitingList_.get(0));
				bucketWaitingList_.remove(0);
				if (!bucketWaitingList_.isEmpty()) {
					messageSender_.sendMessageWithReply(bucketWaitingList_.get(0).getSocketAddress(),
							new PingMessage(getLocalNodeInfo(), bucketWaitingList_.get(0)), 
							this);
				}
			}
		}
	}

	private void addPeersToBuckets(Collection<NodeInfo> initialKnownPeers) {
		List<List<NodeInfo>> tempBuckets = initializeKBuckets();
		for (NodeInfo nodeInfo: initialKnownPeers) {
			if (!nodeInfo.getKey().equals(localKey_)) {
				tempBuckets.get(getDistanceBitFromLocalKey(nodeInfo.getKey())).add(nodeInfo);
			}/* Assign to Buckets */
			
		}
		/* Randomize each bucket and put into real one */
		for (int i = 0; i < Key.KEY_LENGTH; ++i) {
			List<NodeInfo> tempBucket = tempBuckets.get(i);
			Collections.shuffle(tempBuckets.get(i), random_);
			kBuckets_.get(i).addAll(
					tempBucket.subList(0, Math.min(tempBucket.size(), k_)));
		}
	}
	
	private void changeAddress(InetSocketAddress newAddress) {
		synchronized(localKey_) {
			localAddress_ = newAddress;
		}
	}


	private void checkKeysOfUnknownPeers(Collection<InetSocketAddress> initialPeerAddresses) {
		/* TODO */
	}


	private void clearBuckets() {
		for (int i = 0; i < Key.KEY_LENGTH; ++i) {
			kBuckets_.get(i).clear();
		}
	}


	private int findIndexOfNodeInfo(List<NodeInfo> bucket, Key key) {
		int i = 0;
		for (NodeInfo nodeInfo: bucket) {
			if (nodeInfo.getKey().equals(key)) {
				return i;
			}
			++i;
		}
		return -1;
	}


	private Collection<NodeInfo> getClosestRoutingTableNodes(final Key key, int size) {
		KeyComparator keyComparator = new KeyComparator(key);
		SortedSet<Key> closestKeys = new BoundedSortedSet<>(new TreeSet<>(keyComparator), size);
		Map<Key, NodeInfo> keyInfoMap_ = new HashMap<>();
		
		for (List<NodeInfo> bucket: kBuckets_) {
			synchronized(bucket) {
				for (NodeInfo nodeInfo: bucket) {
					keyInfoMap_.put(nodeInfo.getKey(), nodeInfo);
					closestKeys.add(nodeInfo.getKey());
				}
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
	
	private int getDistanceBitFromLocalKey(Key key) {
		BitSet distance = localKey_.calculateDistance(key);
		return distance.previousSetBit(distance.length() - 1);
		
	}

	private NodeInfo getLocalNodeInfo() {
		synchronized (localKey_) {
			return new NodeInfo(localKey_, localAddress_);
		}
	}


	private List<List<NodeInfo>> initializeKBuckets() {
		List<List<NodeInfo>> kBuckets = new ArrayList<>(Key.KEY_LENGTH);
		for (int i = 0; i < Key.KEY_LENGTH; ++i) {
			kBuckets.add(i, new LinkedList<NodeInfo>());
		}
		return kBuckets;
	}

	/**
	 * Add (if possible) information from sender of a message.
	 * 
	 * @param sourceNodeInfo
	 */
	private void processSenderNodeInfo(NodeInfo sourceNodeInfo) {
		LOGGER.trace("processSenderNodeInfo({})", sourceNodeInfo);
		if (sourceNodeInfo.getKey().equals(localKey_)) {
			return;
		}
		int distBit = getDistanceBitFromLocalKey(sourceNodeInfo.getKey());
		List<NodeInfo> bucket = kBuckets_.get(distBit);
		synchronized (bucket) {
			int elementIndex = findIndexOfNodeInfo(bucket, sourceNodeInfo.getKey());
			if (elementIndex == -1) {
				if (bucket.size() < k_) {
					LOGGER.trace("processSenderNodeInfo() -> bucket.add({})", sourceNodeInfo);
					bucket.add(sourceNodeInfo);
				} else {
					NodeInfo firstNodeInfo = bucket.get(0);
					List<NodeInfo> waitingBucket = kBucketsWaitingList_.get(distBit);
					waitingBucket.add(sourceNodeInfo);
					if (waitingBucket.isEmpty()) {
						messageSender_.sendMessageWithReply(firstNodeInfo.getSocketAddress(),
								new PingMessage(getLocalNodeInfo(), firstNodeInfo), 
								new PingCheckMessageHandler(distBit));
					}
				}
			} else {
				LOGGER.trace("processSenderNodeInfo() -> replaceNodeInfo({}, {})",
						elementIndex, sourceNodeInfo);
				replaceNodeInfo(bucket, elementIndex, sourceNodeInfo);
			}
		}
	}
	
	/**
	 * Replaces node info in given bucket. Assumes we have lock on that bucket.
	 * 
	 * @param bucket_
	 * @param indexToReplace
	 * @param newNodeInfo
	 */
	private void replaceNodeInfo(List<NodeInfo> bucket_, int indexToReplace,
			NodeInfo newNodeInfo) {
		bucket_.remove(indexToReplace);
		bucket_.add(newNodeInfo);
	}
}