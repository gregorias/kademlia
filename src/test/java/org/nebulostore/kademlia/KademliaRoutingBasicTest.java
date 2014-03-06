package org.nebulostore.kademlia;

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
import static org.junit.Assert.assertTrue;

public final class KademliaRoutingBasicTest {
	private static final Random RANDOM = new Random();
	private ScheduledExecutorService scheduledExecutor_;
	private KademliaRoutingBuilder builder_;
	private LocalMessaging localMessaging_;

	@Before
	public void setUp() throws KademliaException {
		localMessaging_ = new LocalMessaging();
		
		scheduledExecutor_ = Executors.newScheduledThreadPool(1);

		builder_ = new KademliaRoutingBuilder(RANDOM);

		builder_.setByteListeningService(localMessaging_.getByteListeningService());
		builder_.setByteSender(localMessaging_.getByteSender());
		builder_.setExecutor(scheduledExecutor_);
		builder_.setNetworkAddressDiscovery(localMessaging_.getNetworkAddressDiscovery());
	}

	@After
	public void tearDown() throws KademliaException {
	}
	
	@Test
	public void kademliaPeersShouldFindEachOther() throws KademliaException, InterruptedException {
		Key key0 = new Key(0);
		Key key1 = new Key(1);
		builder_.setKey(key0);
		KademliaRouting kademlia0 = builder_.createPeer();
		Collection<NodeInfo> peerInfos = new LinkedList<>();
		peerInfos .add(new NodeInfo(key0,
				localMessaging_.getNetworkAddressDiscovery().getNetworkAddress()));
		builder_.setInitialPeersWithKeys(peerInfos);
		builder_.setKey(key1);
		KademliaRouting kademlia1 = builder_.createPeer();
		
		kademlia0.start();
		kademlia1.start();

		Collection<NodeInfo> foundZeros = kademlia1.findClosestNodes(key0);
		boolean foundZero = false;
		for (NodeInfo nodeInfo: foundZeros) {
			if (nodeInfo.getKey().equals(key1)) {
				foundZero = true;
			}
		}
		assertTrue(foundZero);
		
		Collection<NodeInfo> foundOnes = kademlia0.findClosestNodes(key1);
		boolean foundOne = false;
		for (NodeInfo nodeInfo: foundOnes) {
			if (nodeInfo.getKey().equals(key1)) {
				foundOne = true;
			}
		}
		assertTrue(foundOne);
		
		kademlia1.stop();
		kademlia0.stop();
	}

	@Test
	public void kademliaPeersShouldFindItSelfWhenLookingForItself() throws KademliaException, InterruptedException {
		Key key0 = new Key(0);
		builder_.setKey(key0);
		KademliaRouting kademlia = builder_.createPeer();
		kademlia.start();

		Collection<NodeInfo> foundNodes = kademlia.findClosestNodes(key0);
		assertEquals(1, foundNodes.size());
		for (NodeInfo nodeInfo: foundNodes) {
			assertEquals(key0, nodeInfo.getKey());
		}
		kademlia.stop();
	}

	@Test
	public void kademliaPeersShouldFindItSelfWhenLookingForOther() throws KademliaException, InterruptedException {
		Key key0 = new Key(0);
		Key key10 = new Key(10);
		builder_.setKey(key0);
		KademliaRouting kademlia = builder_.createPeer();
		kademlia.start();

		Collection<NodeInfo> foundNodes = kademlia.findClosestNodes(key10);
		assertEquals(1, foundNodes.size());
		for (NodeInfo nodeInfo: foundNodes) {
			assertEquals(key0, nodeInfo.getKey());
		}
		kademlia.stop();
	}

	@Test
	public void kademliaPeersShouldFindSoughNode() throws KademliaException, InterruptedException {
		Key key0 = new Key(0);
		Key key1 = new Key(1);
		Key key2 = new Key(2);
		builder_.setKey(key0);
		KademliaRouting kademlia0 = builder_.createPeer();
		Collection<NodeInfo> peerInfos = new LinkedList<>();
		peerInfos .add(new NodeInfo(key0,
				localMessaging_.getNetworkAddressDiscovery().getNetworkAddress()));
		builder_.setInitialPeersWithKeys(peerInfos);
		builder_.setKey(key1);
		KademliaRouting kademlia1 = builder_.createPeer();
		builder_.setKey(key2);
		KademliaRouting kademlia2 = builder_.createPeer();
		
		kademlia0.start();
		kademlia1.start();
		kademlia2.start();

		kademlia1.findClosestNodes(key0);
		kademlia2.findClosestNodes(key0);

		Collection<NodeInfo> foundNodes = kademlia0.findClosestNodes(key2);
		boolean hasFound2 = false;
		for (NodeInfo nodeInfo: foundNodes) {
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
	public void kademliaPeersShouldStartAndStopMultipleTimes() throws KademliaException, InterruptedException {
		Key key0 = new Key(0);
		builder_.setKey(key0);
		KademliaRouting kademlia = builder_.createPeer();
		kademlia.start();

		Collection<NodeInfo> foundNodes = kademlia.findClosestNodes(key0);
		assertEquals(1, foundNodes.size());
		for (NodeInfo nodeInfo: foundNodes) {
			assertEquals(key0, nodeInfo.getKey());
		}
		kademlia.stop();

		kademlia.start();

		foundNodes = kademlia.findClosestNodes(key0);
		assertEquals(1, foundNodes.size());
		for (NodeInfo nodeInfo: foundNodes) {
			assertEquals(key0, nodeInfo.getKey());
		}
		kademlia.stop();

		kademlia.start();

		foundNodes = kademlia.findClosestNodes(key0);
		assertEquals(1, foundNodes.size());
		for (NodeInfo nodeInfo: foundNodes) {
			assertEquals(key0, nodeInfo.getKey());
		}
		kademlia.stop();
	}
	
	@Test
	public void shouldReturnLocalKey() {
		Key key = new Key(0);
		builder_.setKey(key);
		KademliaRouting kademlia = builder_.createPeer();
		assertEquals(key, kademlia.getLocalKey());
	}
}
