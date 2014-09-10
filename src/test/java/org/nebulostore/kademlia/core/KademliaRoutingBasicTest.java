package org.nebulostore.kademlia.core;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.nebulostore.kademlia.core.KademliaException;
import org.nebulostore.kademlia.core.KademliaRouting;
import org.nebulostore.kademlia.core.KademliaRoutingBuilder;
import org.nebulostore.kademlia.core.Key;
import org.nebulostore.kademlia.core.NodeInfo;
import org.nebulostore.kademlia.network.local.LocalMessaging;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public final class KademliaRoutingBasicTest {
  private static final Random RANDOM = new Random();
  private ScheduledExecutorService mScheduledExecutor;
  private KademliaRoutingBuilder mBuilder;
  private LocalMessaging mLocalMessaging;

  @Before
  public void setUp() throws KademliaException {
    mLocalMessaging = new LocalMessaging();

    mScheduledExecutor = Executors.newScheduledThreadPool(1);

    mBuilder = new KademliaRoutingBuilder(RANDOM);

    mBuilder.setByteListeningService(mLocalMessaging.getByteListeningService(0));
    mBuilder.setByteSender(mLocalMessaging.getByteSender());
    mBuilder.setExecutor(mScheduledExecutor);
    mBuilder.setNetworkAddressDiscovery(mLocalMessaging.getNetworkAddressDiscovery(0));
  }

  @After
  public void tearDown() throws KademliaException {
  }

  @Test
  public void kademliaPeersShouldFindEachOther() throws KademliaException, InterruptedException {
    Key key0 = new Key(0);
    Key key1 = new Key(1);
    mBuilder.setKey(key0);
    KademliaRouting kademlia0 = mBuilder.createPeer();
    Collection<NodeInfo> peerInfos = new LinkedList<>();
    peerInfos.add(new NodeInfo(key0, mLocalMessaging.getNetworkAddressDiscovery(0)
        .getNetworkAddress()));
    mBuilder.setInitialPeersWithKeys(peerInfos);
    mBuilder.setKey(key1);
    KademliaRouting kademlia1 = mBuilder.createPeer();

    kademlia0.start();
    kademlia1.start();

    Collection<NodeInfo> foundZeros = kademlia1.findClosestNodes(key0);
    boolean foundZero = false;
    for (NodeInfo nodeInfo : foundZeros) {
      if (nodeInfo.getKey().equals(key1)) {
        foundZero = true;
      }
    }
    assertTrue(foundZero);

    Collection<NodeInfo> foundOnes = kademlia0.findClosestNodes(key1);
    boolean foundOne = false;
    for (NodeInfo nodeInfo : foundOnes) {
      if (nodeInfo.getKey().equals(key1)) {
        foundOne = true;
      }
    }
    assertTrue(foundOne);

    kademlia1.stop();
    kademlia0.stop();
  }

  @Test
  public void kademliaPeersShouldFindItSelfWhenLookingForItself() throws KademliaException,
      InterruptedException {
    Key key0 = new Key(0);
    mBuilder.setKey(key0);
    KademliaRouting kademlia = mBuilder.createPeer();
    kademlia.start();

    Collection<NodeInfo> foundNodes = kademlia.findClosestNodes(key0);
    assertEquals(1, foundNodes.size());
    for (NodeInfo nodeInfo : foundNodes) {
      assertEquals(key0, nodeInfo.getKey());
    }
    kademlia.stop();
  }

  @Test
  public void kademliaPeersShouldFindItSelfWhenLookingForOther() throws KademliaException,
      InterruptedException {
    Key key0 = new Key(0);
    Key key10 = new Key(10);
    mBuilder.setKey(key0);
    KademliaRouting kademlia = mBuilder.createPeer();
    kademlia.start();

    Collection<NodeInfo> foundNodes = kademlia.findClosestNodes(key10);
    assertEquals(1, foundNodes.size());
    for (NodeInfo nodeInfo : foundNodes) {
      assertEquals(key0, nodeInfo.getKey());
    }
    kademlia.stop();
  }

  @Test
  public void kademliaPeersShouldFindSoughNode() throws KademliaException, InterruptedException {
    Key key0 = new Key(0);
    Key key1 = new Key(1);
    Key key2 = new Key(2);
    mBuilder.setKey(key0);
    KademliaRouting kademlia0 = mBuilder.createPeer();
    Collection<NodeInfo> peerInfos = new LinkedList<>();
    peerInfos.add(new NodeInfo(key0, mLocalMessaging.getNetworkAddressDiscovery(0)
        .getNetworkAddress()));
    mBuilder.setInitialPeersWithKeys(peerInfos);
    mBuilder.setKey(key1);
    KademliaRouting kademlia1 = mBuilder.createPeer();
    mBuilder.setKey(key2);
    KademliaRouting kademlia2 = mBuilder.createPeer();

    kademlia0.start();
    kademlia1.start();
    kademlia2.start();

    kademlia1.findClosestNodes(key0);
    kademlia2.findClosestNodes(key0);

    Collection<NodeInfo> foundNodes = kademlia0.findClosestNodes(key2);
    boolean hasFound2 = false;
    for (NodeInfo nodeInfo : foundNodes) {
      if (nodeInfo.getKey().equals(key2)) {
        hasFound2 = true;
      }
    }
    assertTrue(hasFound2);
    kademlia2.stop();
    kademlia1.stop();
    kademlia0.stop();
  }

  @Test
  public void kademliaPeersShouldStartAndStopMultipleTimes() throws KademliaException,
      InterruptedException {
    Key key0 = new Key(0);
    mBuilder.setKey(key0);
    KademliaRouting kademlia = mBuilder.createPeer();
    kademlia.start();

    Collection<NodeInfo> foundNodes = kademlia.findClosestNodes(key0);
    assertEquals(1, foundNodes.size());
    for (NodeInfo nodeInfo : foundNodes) {
      assertEquals(key0, nodeInfo.getKey());
    }
    kademlia.stop();

    kademlia.start();

    foundNodes = kademlia.findClosestNodes(key0);
    assertEquals(1, foundNodes.size());
    for (NodeInfo nodeInfo : foundNodes) {
      assertEquals(key0, nodeInfo.getKey());
    }
    kademlia.stop();

    kademlia.start();

    foundNodes = kademlia.findClosestNodes(key0);
    assertEquals(1, foundNodes.size());
    for (NodeInfo nodeInfo : foundNodes) {
      assertEquals(key0, nodeInfo.getKey());
    }
    kademlia.stop();
  }

  @Test
  public void shouldGetUnknownKey() throws KademliaException {
    Key key0 = new Key(0);
    Key key1 = new Key(1);

    Collection<InetSocketAddress> peerAddresses = new LinkedList<>();
    peerAddresses.add(mLocalMessaging.getNetworkAddressDiscovery(0).getNetworkAddress());

    mBuilder.setKey(key0);
    KademliaRouting kademlia0 = mBuilder.createPeer();
    kademlia0.start();

    mBuilder.setKey(key1);
    mBuilder.setByteListeningService(mLocalMessaging.getByteListeningService(1));
    mBuilder.setNetworkAddressDiscovery(mLocalMessaging.getNetworkAddressDiscovery(1));
    mBuilder.setInitialPeersWithoutKeys(peerAddresses);
    KademliaRouting kademlia1 = mBuilder.createPeer();
    kademlia1.start();

    assertTrue(doesRoutingTableContainKey(kademlia1.getRoutingTable(), key0));
    kademlia1.stop();
    kademlia0.stop();
  }

  @Test
  public void shouldRefreshRoutingTable() throws InterruptedException, KademliaException,
      IOException {
    Key key0 = new Key(0);
    Key key2 = new Key(2);
    Key key3 = new Key(3);

    Collection<NodeInfo> peerInfos = new LinkedList<>();
    peerInfos.add(new NodeInfo(key0, mLocalMessaging.getNetworkAddressDiscovery(0)
        .getNetworkAddress()));

    mBuilder.setKey(key0);
    mBuilder.setBucketSize(1);
    mBuilder.setEntryRefreshingDelay(100);
    KademliaRouting kademlia0 = mBuilder.createPeer();

    StaticKademliaRoutingImpl kademlia2 = newStaticKademlia(2, 1, peerInfos);
    StaticKademliaRoutingImpl kademlia3 = newStaticKademlia(3, 1, peerInfos);
    kademlia0.start();
    kademlia2.start();

    WaitingMessageResponseHandler waiter = new WaitingMessageResponseHandler();
    kademlia2.sendPingToNode(key0, waiter);
    waiter.waitForResponse();

    Collection<NodeInfo> routingTable = kademlia0.getRoutingTable();
    assertTrue(doesRoutingTableContainKey(routingTable, key2));
    kademlia2.stop();
    mBuilder.setKey(key3);
    kademlia3.start();

    waiter = new WaitingMessageResponseHandler();
    WaitingForMessageListener pingListener = new WaitingForMessageListener();
    kademlia3.setMessageListenerAdditionalActions(pingListener);
    pingListener.initializeCatchingTheNextMessage();
    kademlia3.sendPingToNode(key0, waiter);
    waiter.waitForResponse();
    pingListener.waitForMessage();

    routingTable = kademlia0.getRoutingTable();
    assertFalse(doesRoutingTableContainKey(routingTable, key2));
    assertTrue(doesRoutingTableContainKey(routingTable, key3));
    kademlia3.stop();
    kademlia0.stop();
  }

  @Test
  public void shouldReturnLocalKey() {
    Key key = new Key(0);
    mBuilder.setKey(key);
    KademliaRouting kademlia = mBuilder.createPeer();
    assertEquals(key, kademlia.getLocalKey());
  }

  private boolean doesRoutingTableContainKey(Collection<NodeInfo> infos, Key key) {
    for (NodeInfo info : infos) {
      if (info.getKey().equals(key)) {
        return true;
      }
    }
    return false;
  }

  private StaticKademliaRoutingImpl newStaticKademlia(int nr,
      int bucketSize,
      Collection<NodeInfo> knownPeers) {
    return new StaticKademliaRoutingImpl(new NodeInfo(new Key(nr),
        mLocalMessaging.getNetworkAddressDiscovery(nr).getNetworkAddress()),
        new MessageSenderAdapter(mLocalMessaging.getByteSender()),
        new MessageListeningServiceAdapter(mLocalMessaging.getByteListeningService(nr)),
        bucketSize,
        knownPeers);
  }

}
