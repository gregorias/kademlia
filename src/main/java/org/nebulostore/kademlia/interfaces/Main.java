package org.nebulostore.kademlia.interfaces;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.nebulostore.kademlia.KademliaException;
import org.nebulostore.kademlia.KademliaRouting;
import org.nebulostore.kademlia.KademliaRoutingBuilder;
import org.nebulostore.kademlia.Key;
import org.nebulostore.kademlia.NodeInfo;
import org.nebulostore.kademlia.interfaces.rest.RestApp;
import org.nebulostore.kademlia.network.UserGivenNetworkAddressDiscovery;
import org.nebulostore.kademlia.network.socket.SimpleSocketByteListeningService;
import org.nebulostore.kademlia.network.socket.SimpleSocketByteSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
	private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);
	public static void main(String[] args) throws UnknownHostException {
		if (args.length < 5) {
			System.out.println("Usage: Main HTTP_PORT LOCAL_LISTENING_PORT"
					+ " KEY_0_LISTENING_PORT LOCAL_KEY KEY_0_ADDR [LOCAL_ADDR]");
			return;
		}
		LOGGER.info("main({}, {}, {}, {}, {})", args[0], args[1], args[2], args[3], args[4]);

		final URI baseURI = URI.create(String.format("http://localhost:%s/", args[0]));
		final Key localKey = new Key(Integer.parseInt(args[3]));
		InetAddress localInetAddress;
		if (args.length == 6) {
			localInetAddress = InetAddress.getByName(args[5]);
		} else {
			localInetAddress = InetAddress.getLocalHost();
		}
		final ScheduledExecutorService scheduledExecutor = Executors.newScheduledThreadPool(1);
		final ExecutorService executor = Executors.newFixedThreadPool(1);

		KademliaRoutingBuilder builder = new KademliaRoutingBuilder(new Random());
		builder.setBucketSize(2);
		SimpleSocketByteListeningService ssbls = new SimpleSocketByteListeningService(
				Integer.parseInt(args[1]), executor);
		try {
			ssbls.start();
		} catch (IOException e) {
			LOGGER.error("main() -> could not create listening service.", e);
			return;
		}
		builder.setByteListeningService(ssbls);
		builder.setByteSender(new SimpleSocketByteSender());
		builder.setExecutor(scheduledExecutor);
		Collection<NodeInfo> peersWithKnownAddresses = new LinkedList<>();
		if (!localKey.equals(new Key(0))) {
			peersWithKnownAddresses.add(new NodeInfo(
					new Key(0), new InetSocketAddress(args[4], Integer.parseInt(args[2]))));
		}
		builder.setInitialPeersWithKeys(peersWithKnownAddresses);
		builder.setKey(localKey);
		builder.setBucketSize(3);
		builder.setNetworkAddressDiscovery(
				new UserGivenNetworkAddressDiscovery(
						new InetSocketAddress(localInetAddress, Integer.parseInt(args[1]))));
		
		KademliaRouting kademlia = builder.createPeer();
		RestApp app = new RestApp(kademlia, baseURI);
		try {
			kademlia.start();
		} catch (KademliaException e) {
			LOGGER.error("main() -> kademlia.start()", e);
			return;
		}
		app.run();
		if (kademlia.isRunning()) {
			try {
				kademlia.stop();
			} catch (KademliaException e) {
				LOGGER.error("main() -> kademlia.stop()", e);
			}
		}
		ssbls.stop();
		try {
			LOGGER.debug("main(): executor.shutdown()");
			executor.shutdown();
			executor.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
			LOGGER.debug("main(): scheduledExecutor.shutdown()");
			scheduledExecutor.shutdown();
			scheduledExecutor.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			LOGGER.error("main() -> unexpected interrupt", e);
		}
		LOGGER.info("main(): void");
	}
}
