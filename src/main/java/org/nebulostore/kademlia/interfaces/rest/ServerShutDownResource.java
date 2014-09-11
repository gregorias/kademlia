package org.nebulostore.kademlia.interfaces.rest;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shut down entire REST application.
 *
 * @author Grzegorz Milka
 */
@Path("shut_down")
public final class ServerShutDownResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServerShutDownResource.class);
  private final Lock mLock;
  private final Condition mCondition;
  private final AtomicBoolean mHasShutdownBeenCalled;

  public ServerShutDownResource(Lock lock,
      Condition condition,
      AtomicBoolean hasShutdownBeenCalled) {
    assert !hasShutdownBeenCalled.get();
    mLock = lock;
    mCondition = condition;
    mHasShutdownBeenCalled = hasShutdownBeenCalled;
  }

  @POST
  @Produces(MediaType.TEXT_PLAIN)
  public Response shutDown() {
    LOGGER.info("shutDown()");
    mLock.lock();
    try {
      if (mHasShutdownBeenCalled.get()) {
        throw new IllegalStateException("Server has been ordered to shut down already.");
      }
      mHasShutdownBeenCalled.set(true);
      mCondition.signal();
      LOGGER.info("shut_down() -> ok");
      return Response.ok().build();
    } catch (Exception e) {
      LOGGER.info("shutDown() -> bad request");
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(String.format("Could not shut down: %s.", e)).build();
    } finally {
      mLock.unlock();
    }
  }
}