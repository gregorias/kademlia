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

/**
 * {@link KademliaRouting} implementation which has static routing table.
 *
 * @author Grzegorz Milka
 */
class StaticKademliaRoutingImpl implements KademliaRouting {
  private static final Logger LOGGER = LoggerFactory.getLogger(KademliaRoutingImpl.class);

  private final List<NodeInfo> mBucket;

  private final Key mLocalKey;
  private InetSocketAddress mLocalAddress;
  private final MessageSender mMessageSender;
  private final ListeningService mListeningService;
  private final MessageListener mMessageListener;
  private final int mBucketSize;

  private final Lock mReadRunningLock;
  private final Lock mWriteRunningLock;

  private boolean mIsRunning = false;

  private MessageListener mAuxListener;

  StaticKademliaRoutingImpl(NodeInfo localNodeInfo, MessageSender sender,
      ListeningService listeningService, int bucketSize, Collection<NodeInfo> knownPeers) {
    assert bucketSize > 0;
    mBucketSize = bucketSize;
    mBucket = new LinkedList<>(knownPeers);
    mLocalKey = localNodeInfo.getKey();
    mLocalAddress = localNodeInfo.getSocketAddress();
    mMessageSender = sender;
    mListeningService = listeningService;
    mMessageListener = new MessageListenerImpl();

    ReadWriteLock rwLock = new ReentrantReadWriteLock();
    mReadRunningLock = rwLock.readLock();
    mWriteRunningLock = rwLock.writeLock();
  }

  @Override
  public Collection<NodeInfo> findClosestNodes(Key key) {
    return findClosestNodes(key, 0);
  }

  @Override
  public Collection<NodeInfo> findClosestNodes(Key key, int size) {
    throw new UnsupportedOperationException("findClosestNodes is unsupported for this"
        + " fake implementation");
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
    Collection<NodeInfo> routingTable = new ArrayList<>(mBucket);
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
        nodeInfos.add(mBucket.get(index));
      } else {
        throw new IllegalArgumentException(String.format("Key: %s is not present in"
            + " routing table.", key));
      }
    }
    sendPingsToNodes(nodeInfos, handler);
  }

  public synchronized void setMessageListenerAdditionalActions(MessageListener listener) {
    mAuxListener = listener;
  }

  @Override
  public void start() throws KademliaException {
    mWriteRunningLock.lock();
    try {
      LOGGER.info("start()");
      if (mIsRunning) {
        throw new IllegalStateException("Kademlia has already started.");
      }

      LOGGER.trace("startUp() -> registerListener");
      mListeningService.registerListener(mMessageListener);
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
      mListeningService.unregisterListener(mMessageListener);
      mIsRunning = false;
    } finally {
      mWriteRunningLock.unlock();
    }
    LOGGER.info("stop(): void");
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

      int idx = b1.length();
      while (idx > 0 && b1.previousSetBit(idx - 1) == b2.previousSetBit(idx - 1)) {
        idx = b1.previousSetBit(idx - 1);
      }
      if (idx == 0 || idx == -1) {
        return 0;
      } else {
        assert idx > 0;
        return Integer.compare(b1.previousSetBit(idx - 1), b2.previousSetBit(idx - 1));
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
      synchronized (StaticKademliaRoutingImpl.this) {
        if (mAuxListener != null) {
          mAuxListener.receiveFindNodeMessage(msg);
        }
      }
      return new FindNodeReplyMessage(getLocalNodeInfo(), msg.getSourceNodeInfo(),
          getClosestRoutingTableNodes(msg.getSearchedKey(), mBucketSize));
    }

    @Override
    public PongMessage receiveGetKeyMessage(GetKeyMessage msg) {
      synchronized (StaticKademliaRoutingImpl.this) {
        if (mAuxListener != null) {
          mAuxListener.receiveGetKeyMessage(msg);
        }
      }
      return new PongMessage(getLocalNodeInfo(), msg.getSourceNodeInfo());
    }

    @Override
    public PongMessage receivePingMessage(PingMessage msg) {
      synchronized (StaticKademliaRoutingImpl.this) {
        if (mAuxListener != null) {
          mAuxListener.receivePingMessage(msg);
        }
      }
      return new PongMessage(getLocalNodeInfo(), msg.getSourceNodeInfo());
    }
  }

  private int findIndexOfNodeInfo(Key key) {
    int idx = 0;
    synchronized (mBucket) {
      for (NodeInfo nodeInfo : mBucket) {
        if (nodeInfo.getKey().equals(key)) {
          return idx;
        }
        ++idx;
      }
    }
    return -1;
  }

  private Collection<NodeInfo> getClosestRoutingTableNodes(final Key key, int size) {
    KeyComparator keyComparator = new KeyComparator(key);
    SortedSet<Key> closestKeys = new BoundedSortedSet<>(new TreeSet<>(keyComparator), size);
    Map<Key, NodeInfo> keyInfoMap = new HashMap<>();

    synchronized (mBucket) {
      for (NodeInfo nodeInfo : mBucket) {
        keyInfoMap.put(nodeInfo.getKey(), nodeInfo);
        closestKeys.add(nodeInfo.getKey());
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

  private void sendPingsToNodes(Collection<NodeInfo> routingTableNodes,
      MessageResponseHandler handler) {
    for (NodeInfo info : routingTableNodes) {
      mMessageSender.sendMessageWithReply(info.getSocketAddress(), new PingMessage(
          getLocalNodeInfo(), info), handler);
    }
  }
}
