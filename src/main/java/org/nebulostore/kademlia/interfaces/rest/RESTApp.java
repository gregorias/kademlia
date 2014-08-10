package org.nebulostore.kademlia.interfaces.rest;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.nebulostore.kademlia.core.KademliaRouting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * REST interface for kademlia.
 * 
 * @author Grzegorz Milka
 */
public class RESTApp {
	private static final Logger LOGGER = LoggerFactory.getLogger(RESTApp.class);
	private final KademliaRouting kademlia_;
	private final URI uri_;
	private final Lock lock_;
	private final Condition shutDownCondition_;
	private final AtomicBoolean hasShutDownBeenCalled_;

	public RESTApp(KademliaRouting kademlia, URI uri) {
		kademlia_ = kademlia;
		uri_ = uri;
		lock_ = new ReentrantLock();
		shutDownCondition_ = lock_.newCondition();
		hasShutDownBeenCalled_ = new AtomicBoolean(false);
	}
	
	public void run() {
		LOGGER.info("run()");
		ResourceConfig config = createConfig();
    final HttpServer server = GrizzlyHttpServerFactory.createHttpServer(uri_, config);
    try {
			server.start();
			lock_.lock();
			try {
				while (!hasShutDownBeenCalled_.get()) {
					shutDownCondition_.await();
				}
		    server.shutdown();
			} catch (InterruptedException e) {
				server.shutdownNow();
				LOGGER.error("run() -> Unexpected exception.", e);
				return;
			} finally {
				lock_.unlock();
			}
		} catch (IOException e) {
			LOGGER.error("run() -> IOException .", e);
		}
        LOGGER.info("run(): void");
	}
	
	private ResourceConfig createConfig() {
		ResourceConfig config = new ResourceConfig();
		config.register(new KademliaStartResource(kademlia_));
		config.register(new GetLocalKeyResource(kademlia_));
		config.register(new FindNodesResource(kademlia_));
		config.register(new KademliaGetRoutingTableResource(kademlia_));
		config.register(new KademliaStopResource(kademlia_));
		config.register(new ServerShutDownResource(lock_, shutDownCondition_, hasShutDownBeenCalled_));
		return config;
	}

}
