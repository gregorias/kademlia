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
  private final Random mRandom;

  private final Collection<InetSocketAddress> mInitialPeerAddresses;
  private final Collection<NodeInfo> mInitialKnownPeers;

  private final List<List<NodeInfo>> mKBuckets;

  /**
   * List of nodes expecting to be put into a bucket if a place for them is
   * emptied.
   */
  private final List<List<NodeInfo>> mKBucketsWaitingList;
  private final Key mLocalKey;
  private InetSocketAddress mLocalAddress;
  private final MessageSender mMessageSender;
  private final ListeningService mListeningService;
  private final MessageListener mMessageListener;
  private final NetworkAddressDiscovery mNetAddrDiscovery;
  private final int mBucketSize;
  /**
   * Concurrency coefficient in node lookups
   */
  private final int mAlpha;
  private final int mEntryRefreshDelay;

  private final ScheduledExecutorService mScheduledExecutor;

  private final EntryRefresher mEntryRefresher;
  private final AddressChangeObserver mAddrChangeObserver;

  private final Lock mReadRunningLock;
  private final Lock mWriteRunningLock;

  private boolean mIsRunning = false;
  private ScheduledFuture<?> mRefreshFuture = null;

  KademliaRoutingImpl(NodeInfo localNodeInfo,
      MessageSender sender,
      ListeningService listeningService,
      NetworkAddressDiscovery netAddrDiscovery,
      ScheduledExecutorService scheduledExecutor,
      int bucketSize,
      int alpha,
      int entryRefreshDelay,
      Collection<InetSocketAddress> initialPeerAddresses,
      Collection<NodeInfo> initialKnownPeers,
      Random random) {
    assert bucketSize > 0;
    mRandom = random;
    mBucketSize = bucketSize;
    mAlpha = alpha;
    mEntryRefreshDelay = entryRefreshDelay;
    mKBuckets = initializeKBuckets();
    mKBucketsWaitingList = initializeKBuckets();
    mLocalKey = localNodeInfo.getKey();
    mLocalAddress = localNodeInfo.getSocketAddress();
    mMessageSender = sender;
    mListeningService = listeningService;
    mMessageListener = new MessageListenerImpl();
    mNetAddrDiscovery = netAddrDiscovery;

    mScheduledExecutor = scheduledExecutor;

    mInitialPeerAddresses = new LinkedList<>(initialPeerAddresses);
    mInitialKnownPeers = new LinkedList<>(initialKnownPeers);

    mEntryRefresher = new EntryRefresher();
    mAddrChangeObserver = new AddressChangeObserver();

    ReadWriteLock rwLock = new ReentrantReadWriteLock();
    mReadRunningLock = rwLock.readLock();
    mWriteRunningLock = rwLock.writeLock();
  }

  @Override
  public Collection<NodeInfo> findClosestNodes(Key key) throws InterruptedException {
    return findClosestNodes(key, mBucketSize);
  }

  @Override
  public Collection<NodeInfo> findClosestNodes(Key key, int size) throws InterruptedException {
    final KeyComparator findKeyComparator = new KeyComparator(key);
    LOGGER.debug("findClosestKeys({}, {})", key.toString(), size);
    mReadRunningLock.lock();
    try {
      if (!mIsRunning) {
        throw new IllegalStateException("Called findClosestNodes() on nonrunning kademlia.");
      }
      Map<Key, NodeInfo> keyInfoMap = new HashMap<>();
      Set<Key> queriedKeys = new HashSet<>();
      PriorityQueue<Key> unqueriedKeys = new PriorityQueue<>(mBucketSize, findKeyComparator);
      SortedSet<Key> candidateKeys = new BoundedSortedSet<>(new TreeSet<>(findKeyComparator), size);

      BlockingQueue<Future<Message>> replyQueue = new LinkedBlockingQueue<>();
      MessageResponseHandler responseHandler = new QueuedMessageResponseHandler(replyQueue);

      Collection<NodeInfo> closestNodes = getClosestRoutingTableNodes(
          key, Math.max(mAlpha, mBucketSize));

      for (NodeInfo nodeInfo : closestNodes) {
        unqueriedKeys.add(nodeInfo.getKey());
        keyInfoMap.put(nodeInfo.getKey(), nodeInfo);
      }

      int connectionIdx = 0;
      while (connectionIdx < mAlpha && !unqueriedKeys.isEmpty()) {
        Key closestKey = unqueriedKeys.remove();
        NodeInfo nodeInfo = keyInfoMap.get(closestKey);

        queriedKeys.add(closestKey);
        mMessageSender.sendMessageWithReply(nodeInfo.getSocketAddress(), new FindNodeMessage(
            getLocalNodeInfo(), nodeInfo, key), responseHandler);
        connectionIdx += 1;
      }

      while (connectionIdx > 0) {
        Future<Message> future = replyQueue.take();
        connectionIdx -= 1;
        try {
          FindNodeReplyMessage msg = (FindNodeReplyMessage) future.get();
          for (NodeInfo foundNodeInfo : msg.getFoundNodes()) {
            NodeInfo nodeInfoInMap = keyInfoMap.get(foundNodeInfo.getKey());
            if (nodeInfoInMap != null
                && !nodeInfoInMap.getSocketAddress().equals(foundNodeInfo.getSocketAddress())) {
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
            if (candidateKeys.size() < size
                || findKeyComparator.compare(newKey, candidateKeys.last()) < 0) {
              NodeInfo nodeInfo = keyInfoMap.get(newKey);
              mMessageSender.sendMessageWithReply(nodeInfo.getSocketAddress(), new FindNodeMessage(
                  getLocalNodeInfo(), nodeInfo, key), responseHandler);
              connectionIdx += 1;
            }
          }
        } catch (ExecutionException e) {
          LOGGER.debug(
              String.format("findClosestKeys(%s) -> exception when sending a message.",
                  key.toString()), e);
        } catch (ClassCastException e) {
          LOGGER.warn(
              String.format("findClosestKeys(%s) -> received message of wrong type.",
                  key.toString()), e);
        }
      }

      Collection<NodeInfo> closestFoundNodeInfos = new ArrayList<>();
      closestFoundNodeInfos.addAll(keyInfoMap.values());
      LOGGER.debug("findClosestKeys(): {}", closestFoundNodeInfos);
      return closestFoundNodeInfos;
    } finally {
      mReadRunningLock.unlock();
    }
  }

  @Override
  public Key getLocalKey() {
    return mLocalKey;
  }

  public boolean isRunning() {
    mReadRunningLock.lock();
    boolean isRunning = mIsRunning;
    mReadRunningLock.unlock();
    return isRunning;
  }

  @Override
  public Collection<NodeInfo> getRoutingTable() {
    Collection<NodeInfo> routingTable = new ArrayList<>();
    for (Collection<NodeInfo> bucket : mKBuckets) {
      routingTable.addAll(bucket);
    }
    return routingTable;
  }

  @Override
  public void start() throws KademliaException {
    mWriteRunningLock.lock();
    try {
      LOGGER.info("start()");
      if (mIsRunning) {
        throw new IllegalStateException("Kademlia has already started.");
      }

      /* Connect to initial peers */
      clearBuckets();
      LOGGER.trace("startUp() -> addPeersToBuckets");
      addPeersToBuckets(mInitialKnownPeers);
      LOGGER.trace("startUp() -> checkKeysOfUnknownPeers");
      checkKeysOfUnknownPeers(mInitialPeerAddresses);
      mNetAddrDiscovery.addObserver(mAddrChangeObserver);
      LOGGER.trace("startUp() -> registerListener");
      mListeningService.registerListener(mMessageListener);
      mRefreshFuture = mScheduledExecutor.schedule(mEntryRefresher, 0, TimeUnit.MILLISECONDS);
      mIsRunning = true;
    } finally {
      mWriteRunningLock.unlock();
    }
  }

  @Override
  public void stop() throws KademliaException {
    mWriteRunningLock.lock();
    try {
      LOGGER.info("stop()");
      if (!mIsRunning) {
        throw new IllegalStateException("Kademlia is not running.");
      }
      mRefreshFuture.cancel(true);
      mListeningService.unregisterListener(mMessageListener);
      mIsRunning = false;
    } finally {
      mWriteRunningLock.unlock();
    }
    LOGGER.info("stop(): void");
  }

  private class AddressChangeObserver implements Observer {
    @Override
    public void update(Observable arg0, Object arg1) {
      InetSocketAddress newAddress = (InetSocketAddress) arg1;
      changeAddress(newAddress);
      mScheduledExecutor.schedule(mEntryRefresher, 0, TimeUnit.MILLISECONDS);
    }
  }

  /**
   * Comparator of BitSets representing a 160-bit number in a little-endian
   * encoding.
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

      int bitIdx = b1.length();
      while (bitIdx > 0 && b1.previousSetBit(bitIdx - 1) == b2.previousSetBit(bitIdx - 1)) {
        bitIdx = b1.previousSetBit(bitIdx - 1);
      }
      if (bitIdx == 0 || bitIdx == -1) {
        return 0;
      } else {
        assert bitIdx > 0;
        return Integer.compare(b1.previousSetBit(bitIdx - 1), b2.previousSetBit(bitIdx - 1));
      }
    }
  }

  private static class BoundedSortedSet<E> extends AbstractSet<E> implements SortedSet<E> {
    private final SortedSet<E> mBase;
    private final int mUpperCountBound;

    public BoundedSortedSet(SortedSet<E> base, int upperCountBound) {
      mBase = base;
      mUpperCountBound = upperCountBound;
    }

    @Override
    public boolean add(E element) {
      boolean hasBeenAdded = mBase.add(element);
      if (!hasBeenAdded) {
        return hasBeenAdded;
      }

      if (mBase.size() > mUpperCountBound) {
        assert mBase.size() == mUpperCountBound + 1;
        E lastElement = mBase.last();
        mBase.remove(lastElement);
        return mBase.comparator().compare(element, lastElement) == 0;
      } else {
        return true;
      }
    }

    @Override
    public Comparator<? super E> comparator() {
      return mBase.comparator();
    }

    @Override
    public E first() {
      return mBase.first();
    }

    @Override
    public SortedSet<E> headSet(E toElement) {
      return mBase.headSet(toElement);
    }

    @Override
    public Iterator<E> iterator() {
      return mBase.iterator();
    }

    @Override
    public E last() {
      return mBase.last();
    }

    @Override
    public int size() {
      return mBase.size();
    }

    @Override
    public SortedSet<E> subSet(E fromElement, E toElement) {
      return mBase.subSet(fromElement, toElement);
    }

    @Override
    public SortedSet<E> tailSet(E fromElement) {
      return mBase.tailSet(fromElement);
    }
  }

  private class EntryRefresher implements Runnable {
    private final Logger mLogger = LoggerFactory.getLogger(EntryRefresher.class);

    @Override
    public synchronized void run() {
      mLogger.debug("run()");
      mReadRunningLock.lock();
      try {
        if (!mIsRunning) {
          return;
        }
        findClosestNodes(mLocalKey);
        mRefreshFuture = mScheduledExecutor.schedule(this, mEntryRefreshDelay,
            TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        mLogger.error("Caught unexpected InterruptedException.", e);
      } finally {
        mReadRunningLock.unlock();
      }
      mLogger.debug("run(): void");
    }

  }

  private class QueuedMessageResponseHandler implements MessageResponseHandler {
    private final BlockingQueue<Future<Message>> mOutputQueue;

    public QueuedMessageResponseHandler(BlockingQueue<Future<Message>> outputQueue) {
      mOutputQueue = outputQueue;
    }

    @Override
    public void onResponse(Message response) {
      SettableFuture<Message> futureResponse = SettableFuture.<Message>create();
      futureResponse.set(response);
      processSenderNodeInfo(response.getSourceNodeInfo());
      mOutputQueue.add(futureResponse);
    }

    @Override
    public void onResponseError(IOException exception) {
      SettableFuture<Message> futureResponse = SettableFuture.<Message>create();
      futureResponse.setException(exception);
      mOutputQueue.add(futureResponse);
    }

    @Override
    public void onSendSuccessful() {
    }

    @Override
    public void onSendError(IOException exception) {
      SettableFuture<Message> futureResponse = SettableFuture.<Message>create();
      futureResponse.setException(exception);
      mOutputQueue.add(futureResponse);
    }
  }

  /**
   * Comparator of Kademlia {@link Key} with specified key based on closeness.
   *
   * @author Grzegorz Milka
   */
  private static class KeyComparator implements Comparator<Key>, Serializable {
    private static final long serialVersionUID = 1L;
    private final Key mReferenceKey;

    public KeyComparator(Key key) {
      mReferenceKey = key;
    }

    @Override
    public int compare(Key arg0, Key arg1) {
      BitSet distance0 = mReferenceKey.calculateDistance(arg0);
      BitSet distance1 = mReferenceKey.calculateDistance(arg1);

      return BitSetComparator.COMPARATOR.compare(distance0, distance1);
    }
  }

  private class MessageListenerImpl implements MessageListener {
    @Override
    public FindNodeReplyMessage receiveFindNodeMessage(FindNodeMessage msg) {
      processSenderNodeInfo(msg.getSourceNodeInfo());
      return new FindNodeReplyMessage(getLocalNodeInfo(), msg.getSourceNodeInfo(),
          getClosestRoutingTableNodes(msg.getSearchedKey(), mBucketSize));
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
    private final int mBucketId;
    private final List<NodeInfo> mBucket;
    private final List<NodeInfo> mBucketWaitingList;

    public PingCheckMessageHandler(int bucketId) {
      mBucketId = bucketId;
      mBucket = mKBuckets.get(mBucketId);
      mBucketWaitingList = mKBucketsWaitingList.get(mBucketId);
    }

    @Override
    public void onResponse(Message response) {
      LOGGER.debug("onResponse()");
      synchronized (mBucket) {
        replaceNodeInfo(mBucket, 0, mBucket.get(0));
        mBucketWaitingList.remove(0);
        checkWaitingQueueAndContinue();
      }
    }

    @Override
    public void onResponseError(IOException exception) {
      LOGGER.debug("onResponseError()", exception);
      onError();
    }

    @Override
    public void onSendSuccessful() {
    }

    @Override
    public void onSendError(IOException exception) {
      LOGGER.debug("onSendError()", exception);
      onError();
    }

    private void checkWaitingQueueAndContinue() {
      if (!mBucketWaitingList.isEmpty()) {
        mMessageSender.sendMessageWithReply(mBucket.get(0).getSocketAddress(), new PingMessage(
            getLocalNodeInfo(), mBucket.get(0)), this);
      }
    }

    private void onError() {
      synchronized (mBucket) {
        replaceNodeInfo(mBucket, 0, mBucketWaitingList.get(0));
        mBucketWaitingList.remove(0);
        checkWaitingQueueAndContinue();
      }
    }
  }

  private void addPeerToBucket(NodeInfo peer) {
    Collection<NodeInfo> peers = new LinkedList<>();
    peers.add(peer);
    addPeersToBuckets(peers);
  }

  private void addPeersToBuckets(Collection<NodeInfo> initialKnownPeers) {
    List<List<NodeInfo>> tempBuckets = initializeKBuckets();
    for (NodeInfo nodeInfo : initialKnownPeers) {
      if (!nodeInfo.getKey().equals(mLocalKey)) {
        tempBuckets.get(getLocalKey().getDistanceBit(nodeInfo.getKey())).add(nodeInfo);
      }
    }
    /* Randomize each bucket and put into real one */
    for (int idx = 0; idx < Key.KEY_LENGTH; ++idx) {
      List<NodeInfo> tempBucket = tempBuckets.get(idx);
      Collections.shuffle(tempBuckets.get(idx), mRandom);
      mKBuckets.get(idx).addAll(
          tempBucket.subList(0, Math.min(
              tempBucket.size(), mBucketSize - mKBuckets.get(idx).size())));
    }
  }

  private void changeAddress(InetSocketAddress newAddress) {
    synchronized (mLocalKey) {
      mLocalAddress = newAddress;
    }
  }

  private void checkKeysOfUnknownPeers(Collection<InetSocketAddress> initialPeerAddresses) {
    GetKeyMessage gkMsg = prepareGetKeyMessage();
    BlockingQueue<Future<Message>> queue = new LinkedBlockingQueue<>();
    MessageResponseHandler queuedMsgResponseHandler = new QueuedMessageResponseHandler(queue);
    for (InetSocketAddress address : initialPeerAddresses) {
      mMessageSender.sendMessageWithReply(address, gkMsg, queuedMsgResponseHandler);
    }

    for (int idx = 0; idx < initialPeerAddresses.size(); ++idx) {
      try {
        Future<Message> future = queue.take();
        PongMessage pong = null;
        try {
          pong = (PongMessage) future.get();
          addPeerToBucket(pong.getSourceNodeInfo());
        } catch (ClassCastException e) {
          LOGGER.warn("checkKeysOfUnknownPeers() -> received message which is not pong: %s", pong);
        } catch (ExecutionException e) {
          LOGGER.info(
              "checkKeysOfUnknownPeers() -> exception happened when trying to get key from host",
              e);
        }
      } catch (InterruptedException e) {
        throw new IllegalStateException("Unexpected interrupt");
      }
    }
  }

  private void clearBuckets() {
    for (int idx = 0; idx < Key.KEY_LENGTH; ++idx) {
      mKBuckets.get(idx).clear();
    }
  }

  private int findIndexOfNodeInfo(List<NodeInfo> bucket, Key key) {
    int idx = 0;
    for (NodeInfo nodeInfo : bucket) {
      if (nodeInfo.getKey().equals(key)) {
        return idx;
      }
      ++idx;
    }
    return -1;
  }

  private Collection<NodeInfo> getClosestRoutingTableNodes(final Key key, int size) {
    KeyComparator keyComparator = new KeyComparator(key);
    SortedSet<Key> closestKeys = new BoundedSortedSet<>(new TreeSet<>(keyComparator), size);
    Map<Key, NodeInfo> keyInfoMap = new HashMap<>();

    for (List<NodeInfo> bucket : mKBuckets) {
      synchronized (bucket) {
        for (NodeInfo nodeInfo : bucket) {
          keyInfoMap.put(nodeInfo.getKey(), nodeInfo);
          closestKeys.add(nodeInfo.getKey());
        }
      }
    }
    keyInfoMap.put(mLocalKey, getLocalNodeInfo());
    closestKeys.add(mLocalKey);

    Collection<NodeInfo> closestNodes = new ArrayList<>();
    for (Key closeKey : closestKeys) {
      closestNodes.add(keyInfoMap.get(closeKey));
    }
    LOGGER.trace("getClosestRoutingTableNodes({}, {}): {}", key, size, closestNodes);
    return closestNodes;
  }

  private NodeInfo getLocalNodeInfo() {
    synchronized (mLocalKey) {
      return new NodeInfo(mLocalKey, mLocalAddress);
    }
  }

  private List<List<NodeInfo>> initializeKBuckets() {
    List<List<NodeInfo>> buckets = new ArrayList<>(Key.KEY_LENGTH);
    for (int idx = 0; idx < Key.KEY_LENGTH; ++idx) {
      buckets.add(idx, new LinkedList<NodeInfo>());
    }
    return buckets;
  }

  private GetKeyMessage prepareGetKeyMessage() {
    return new GetKeyMessage(getLocalNodeInfo());
  }

  /**
   * Add (if possible) information from sender of a message.
   *
   * @param sourceNodeInfo
   */
  private void processSenderNodeInfo(NodeInfo sourceNodeInfo) {
    LOGGER.trace("processSenderNodeInfo({})", sourceNodeInfo);
    if (sourceNodeInfo.getKey().equals(mLocalKey)) {
      return;
    }
    int distBit = getLocalKey().getDistanceBit(sourceNodeInfo.getKey());
    List<NodeInfo> bucket = mKBuckets.get(distBit);
    synchronized (bucket) {
      int elementIndex = findIndexOfNodeInfo(bucket, sourceNodeInfo.getKey());
      if (elementIndex == -1) {
        if (bucket.size() < mBucketSize) {
          LOGGER.trace("processSenderNodeInfo() -> bucket.add({})", sourceNodeInfo);
          bucket.add(sourceNodeInfo);
        } else {
          NodeInfo firstNodeInfo = bucket.get(0);
          List<NodeInfo> waitingBucket = mKBucketsWaitingList.get(distBit);
          if (findIndexOfNodeInfo(waitingBucket, sourceNodeInfo.getKey()) == -1) {
            waitingBucket.add(sourceNodeInfo);
            if (waitingBucket.size() == 1) {
              mMessageSender.sendMessageWithReply(firstNodeInfo.getSocketAddress(),
                  new PingMessage(getLocalNodeInfo(), firstNodeInfo), new PingCheckMessageHandler(
                      distBit));
            }
          }
        }
      } else {
        LOGGER.trace("processSenderNodeInfo() -> replaceNodeInfo({}, {})", elementIndex,
            sourceNodeInfo);
        replaceNodeInfo(bucket, elementIndex, sourceNodeInfo);
      }
    }
  }

  /**
   * Replaces node info in given bucket. Assumes we have lock on that bucket.
   *
   * @param bucket
   * @param indexToReplace
   * @param newNodeInfo
   */
  private void replaceNodeInfo(List<NodeInfo> bucket, int indexToReplace, NodeInfo newNodeInfo) {
    synchronized (bucket) {
      bucket.remove(indexToReplace);
      bucket.add(newNodeInfo);
    }
  }
}