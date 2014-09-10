package org.nebulostore.kademlia.core;

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
import org.nebulostore.kademlia.network.ByteSender;
import org.nebulostore.kademlia.network.NetworkAddressDiscovery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builder of Kademlia routing peers.
 *
 * If you want multiple kademlia peers on the same listening connection you have
 * to: 1. Set {@link ByteListener} only once. 2. Use the same builder for all
 * peers.
 *
 * This class is not thread safe.
 *
 * @author Grzegorz Milka
 */
public class KademliaRoutingBuilder {
  private static final Logger LOGGER = LoggerFactory.getLogger(KademliaRoutingBuilder.class);
  private final Random mRandom;
  private static final int DEFAULT_ENTRY_REFRESH_DELAY = 1000;
  private static final int DEFAULT_BUCKET_SIZE = 10;
  private static final int DEFAULT_ALPHA = 5;

  private ListeningService mListeningAdapter;
  private DemultiplexingMessageListener mDemultiplexingListener;

  private MessageSenderAdapter mMessageSender;
  private NetworkAddressDiscovery mNetAddrDiscovery;
  private ScheduledExecutorService mExecutor;
  private int mBucketSize = DEFAULT_BUCKET_SIZE;
  private int mAlpha = DEFAULT_ALPHA;
  private Key mKey;
  private Collection<InetSocketAddress> mInitialPeersWithoutKeys = new LinkedList<>();
  private Collection<NodeInfo> mInitialPeersWithKeys = new LinkedList<>();
  private int mEntryRefreshDelay = DEFAULT_ENTRY_REFRESH_DELAY;

  public KademliaRoutingBuilder(Random random) {
    mRandom = random;
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
    NodeInfo localNodeInfo = new NodeInfo(usedKey, mNetAddrDiscovery.getNetworkAddress());

    ListeningService listeningService = new MessageListeningServiceImpl(usedKey,
        mDemultiplexingListener);
    LOGGER.debug("createPeer() -> Key: {}", usedKey);
    return new KademliaRoutingImpl(localNodeInfo, mMessageSender, listeningService,
        mNetAddrDiscovery, mExecutor, mBucketSize, mAlpha, mEntryRefreshDelay,
        mInitialPeersWithoutKeys, mInitialPeersWithKeys, mRandom);
  }

  /**
   * Set size of one bucket.
   *
   * @param bucketSize
   *          size of bucket
   * @return this
   */
  public KademliaRoutingBuilder setBucketSize(int bucketSize) {
    mBucketSize = bucketSize;
    return this;
  }

  /**
   * Set {@link ByteSender} used for sending messages.
   *
   * ByteSender may be shared between multiple instances of kademlia.
   *
   * @param byteSender
   *          ByteSender used for sending message
   * @return this
   */
  public KademliaRoutingBuilder setByteSender(ByteSender byteSender) {
    mMessageSender = new MessageSenderAdapter(byteSender);
    return this;
  }

  /**
   * Set {@link ByteListeningService}.
   *
   * ByteListeningService may be shared between multiple instances of kademlia,
   * but they must created from the same builder.
   *
   * @param byteListeningService
   * @return this
   */
  public KademliaRoutingBuilder setByteListeningService(ByteListeningService byteListeningService) {
    mListeningAdapter = new MessageListeningServiceAdapter(byteListeningService);
    mDemultiplexingListener = new DemultiplexingMessageListener(mListeningAdapter);
    return this;
  }

  /**
   * @param alpha
   *          Concurrency parameter used for sending
   * @return this
   */
  public KademliaRoutingBuilder setConcurrencyParameter(int alpha) {
    mAlpha = alpha;
    return this;
  }

  /**
   * Set delay used for periodic refreshing local node in the network.
   *
   * @param delay
   *          delay in milliseconds
   * @return this
   */
  public KademliaRoutingBuilder setEntryRefreshingDelay(int delay) {
    mEntryRefreshDelay = delay;
    return this;
  }

  public KademliaRoutingBuilder setExecutor(ScheduledExecutorService executor) {
    mExecutor = executor;
    return this;
  }

  /**
   * Set initial peers connected to the network, but of unknown Kademlia keys.
   *
   * @param peerAddresses
   * @return this
   */
  public KademliaRoutingBuilder setInitialPeersWithoutKeys(
      Collection<InetSocketAddress> peerAddresses) {
    assert peerAddresses != null;
    mInitialPeersWithoutKeys = new LinkedList<>(peerAddresses);
    return this;
  }

  /**
   * Set initial peers connected to the network with known Kademlia keys.
   *
   * @param peerInfos
   * @return this
   */
  public KademliaRoutingBuilder setInitialPeersWithKeys(Collection<NodeInfo> peerInfos) {
    assert peerInfos != null;
    mInitialPeersWithKeys = new LinkedList<>(peerInfos);
    return this;
  }

  public KademliaRoutingBuilder setKey(Key key) {
    mKey = key;
    return this;
  }

  /**
   * @param netAddrDisc
   * @return this
   */
  public KademliaRoutingBuilder setNetworkAddressDiscovery(NetworkAddressDiscovery netAddrDisc) {
    mNetAddrDiscovery = netAddrDisc;
    return this;
  }

  private static class DemultiplexingMessageListener implements MessageListener {
    private final ListeningService mBaseListeningService;
    private final Map<Key, MessageListener> mListenerMap;
    private final ReadWriteLock mRWLock;
    private final Lock mReadLock;
    private final Lock mWriteLock;

    public DemultiplexingMessageListener(ListeningService baseListeningService) {
      mBaseListeningService = baseListeningService;
      mListenerMap = new HashMap<>();
      mRWLock = new ReentrantReadWriteLock();
      mReadLock = mRWLock.readLock();
      mWriteLock = mRWLock.writeLock();
    }

    public void registerListener(Key key, MessageListener listener) {
      mWriteLock.lock();
      try {
        if (mListenerMap.isEmpty()) {
          mBaseListeningService.registerListener(this);
        }
        if (mListenerMap.containsKey(key)) {
          throw new IllegalStateException(String.format("Kademlia peer at key: %s"
              + " has already registered its listener.", key));
        }
        mListenerMap.put(key, listener);
      } finally {
        mWriteLock.unlock();
      }
    }

    public void unregisterListener(Key key) {
      mWriteLock.lock();
      try {
        if (!mListenerMap.containsKey(key)) {
          throw new IllegalStateException(String.format("Kademlia peer at key: %s"
              + " has no registered listener.", key));
        }
        mListenerMap.remove(key);
        if (mListenerMap.isEmpty()) {
          mBaseListeningService.unregisterListener(this);
        }
      } finally {
        mWriteLock.unlock();
      }
    }

    @Override
    public FindNodeReplyMessage receiveFindNodeMessage(FindNodeMessage msg) {
      mReadLock.lock();
      try {
        MessageListener listener = getRecipient(msg);
        if (listener == null) {
          return null;
        }
        return listener.receiveFindNodeMessage(msg);
      } finally {
        mReadLock.unlock();
      }
    }

    @Override
    public PongMessage receivePingMessage(PingMessage msg) {
      mReadLock.lock();
      try {
        MessageListener listener = getRecipient(msg);
        if (listener == null) {
          return null;
        }
        return listener.receivePingMessage(msg);
      } finally {
        mReadLock.unlock();
      }
    }

    @Override
    public PongMessage receiveGetKeyMessage(GetKeyMessage msg) {
      mReadLock.lock();
      try {
        if (mListenerMap.isEmpty()) {
          LOGGER.warn("receiveGetKeyMessage({}) -> no listener is registered.", msg);
        }
        MessageListener listener = mListenerMap.values().iterator().next();
        return listener.receiveGetKeyMessage(msg);
      } finally {
        mReadLock.unlock();
      }
    }

    private MessageListener getRecipient(MessageWithKnownRecipient msg) {
      Key destKey = msg.getDestinationNodeInfo().getKey();
      MessageListener listener = mListenerMap.get(destKey);
      if (listener == null) {
        LOGGER.debug("getRecipient({}) -> Received message to unknown kademlia peer.", msg);
      }
      return listener;
    }

  }

  private static class MessageListeningServiceImpl implements ListeningService {
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageListeningServiceImpl.class);
    private final DemultiplexingMessageListener mDemux;
    private final Key mKey;

    public MessageListeningServiceImpl(Key key, DemultiplexingMessageListener demux) {
      mDemux = demux;
      mKey = key;
    }

    @Override
    public void registerListener(MessageListener listener) {
      LOGGER.trace("registerListener({})", listener);
      mDemux.registerListener(mKey, listener);
    }

    @Override
    public void unregisterListener(MessageListener listener) {
      LOGGER.trace("unregisterListener({})", listener);
      mDemux.unregisterListener(mKey);
      LOGGER.trace("unregisterListener(): void");
    }
  }

  private void checkIfByteListeningServiceIsSet() {
    if (mListeningAdapter == null) {
      throw new IllegalStateException("Listening service is not set.");
    }
  }

  private void checkIfByteSenderIsSet() {
    if (mMessageSender == null) {
      throw new IllegalStateException("Byte sender is not set.");
    }
  }

  private void checkIfExecutorIsSet() {
    if (mExecutor == null) {
      throw new IllegalStateException("Executor is not set.");
    }
  }

  private void checkIfNetAddrDiscoveryIsSet() {
    if (mNetAddrDiscovery == null) {
      throw new IllegalStateException("Network address discovery is not set.");
    }
  }

  private Key getSetKeyOrCreateNew() {
    if (mKey != null) {
      return mKey;
    } else {
      return Key.newRandomKey(mRandom);
    }
  }
}
