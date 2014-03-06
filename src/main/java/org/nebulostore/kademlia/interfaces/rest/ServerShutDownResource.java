package org.nebulostore.kademlia.interfaces.rest;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("shut_down")
public final class ServerShutDownResource {
	private static final Logger LOGGER = LoggerFactory.getLogger(ServerShutDownResource.class);
	private final Lock lock_;
	private final Condition condition_;
	private final AtomicBoolean hasShutdownBeenCalled_;
	
	public ServerShutDownResource(Lock lock, Condition condition, AtomicBoolean hasShutdownBeenCalled) {
		assert !hasShutdownBeenCalled.get();
		lock_ = lock;
		condition_ = condition;
		hasShutdownBeenCalled_ = hasShutdownBeenCalled;
	}
	
	@GET
	@Produces(MediaType.TEXT_PLAIN) 
	public Response shutDown() {
		LOGGER.info("shutDown()");
		lock_.lock();
        try {
        	if (hasShutdownBeenCalled_.get()) {
        		throw new IllegalStateException("Server has been ordered to shut down already.");
        	}
        	hasShutdownBeenCalled_.set(true);
        	condition_.signal();
			LOGGER.info("shut_down() -> ok");
	        return Response.ok().build();
        } catch (Exception e) {
			LOGGER.info("shutDown() -> bad request");
            return Response.status(Response.Status.BAD_REQUEST).entity(
            		String.format("Could shut down: %s.", e)).build();
        } finally {
        	lock_.unlock();
        }
	}
}