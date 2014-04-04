package org.nebulostore.kademlia.interfaces;

import java.io.File;
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

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.nebulostore.kademlia.core.KademliaException;
import org.nebulostore.kademlia.core.KademliaRouting;
import org.nebulostore.kademlia.core.KademliaRoutingBuilder;
import org.nebulostore.kademlia.core.Key;
import org.nebulostore.kademlia.core.NodeInfo;
import org.nebulostore.kademlia.interfaces.rest.RestApp;
import org.nebulostore.kademlia.network.UserGivenNetworkAddressDiscovery;
import org.nebulostore.kademlia.network.socket.SimpleSocketByteListeningService;
import org.nebulostore.kademlia.network.socket.SimpleSocketByteSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main access point to kademlia interface.
 * 
 * @author Grzegorz Milka
 */
public class Main {
	private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);
	public static void main(String[] args) throws UnknownHostException {
		if (args.length < 1) {
			System.out.println("Usage: Main CONFIG_FILE");
			return;
		}
		LOGGER.info("main({})", args[0]);
		String configFile = args[0];
        XMLConfiguration config = null;
		try {
			config = new XMLConfiguration(new File(configFile));
		} catch (ConfigurationException e) {
			LOGGER.error("main() -> could not read configuration.", e);
			return;
		}
		
        XMLConfiguration kadConfig = config;
		final InetAddress localInetAddress = InetAddress.getByName(kadConfig.getString("local-net-address"));
		final int localPort = kadConfig.getInt("local-net-port");
		final InetAddress hostZeroInetAddress = InetAddress.getByName(kadConfig.getString("bootstrap-net-address"));
		final int hostZeroPort = kadConfig.getInt("bootstrap-net-port");
		final Key localKey = new Key(kadConfig.getInt("local-key"));
		final URI baseURI = URI.create(String.format("http://%s:%s/",
				localInetAddress.getHostName(), kadConfig.getString("rest-port")));

		final ScheduledExecutorService scheduledExecutor = Executors.newScheduledThreadPool(1);
		final ExecutorService executor = Executors.newFixedThreadPool(1);

		KademliaRoutingBuilder builder = new KademliaRoutingBuilder(new Random());
		builder.setBucketSize(2);
		SimpleSocketByteListeningService ssbls = new SimpleSocketByteListeningService(
				localPort, executor);
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
					new Key(0), new InetSocketAddress(hostZeroInetAddress, hostZeroPort)));
		}
		builder.setInitialPeersWithKeys(peersWithKnownAddresses);
		builder.setKey(localKey);
		builder.setBucketSize(3);
		builder.setNetworkAddressDiscovery(new UserGivenNetworkAddressDiscovery(
				new InetSocketAddress(localInetAddress, localPort)));
		
		KademliaRouting kademlia = builder.createPeer();
		RestApp app = new RestApp(kademlia, baseURI);
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
