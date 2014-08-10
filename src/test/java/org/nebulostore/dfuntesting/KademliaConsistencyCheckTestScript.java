package org.nebulostore.dfuntesting;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.nebulostore.dfuntesting.TestResult.Type;
import org.nebulostore.kademlia.core.KademliaException;
import org.nebulostore.kademlia.core.Key;
import org.nebulostore.kademlia.core.NodeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KademliaConsistencyCheckTestScript implements
    TestScript<KademliaApp> {
  private static final Logger LOGGER = LoggerFactory
      .getLogger(KademliaConsistencyCheckTestScript.class);
  private static final int START_UP_DELAY = 5;
  private static final TimeUnit START_UP_DELAY_UNIT = TimeUnit.SECONDS;
  private static final long CHECK_DELAY = 20;
  private static final TimeUnit CHECK_DELAY_UNIT = TimeUnit.SECONDS;
  private static final int CHECK_COUNT = 5;

  private final ScheduledExecutorService scheduledExecutor_;
  private final AtomicBoolean isFinished_;
  private TestResult result_;

  public KademliaConsistencyCheckTestScript(
      ScheduledExecutorService scheduledExecutor) {
    scheduledExecutor_ = scheduledExecutor;
    isFinished_ = new AtomicBoolean(false);
  }

  @Override
  public TestResult run(Collection<KademliaApp> apps) {
    LOGGER.info("run(): Running consistency test on {} hosts.", apps.size());
    Collection<KademliaApp> runningApps = new LinkedList<>();

    try {
      runApps(apps);
      runningApps.addAll(apps);
      try {
        Thread.sleep(START_UP_DELAY_UNIT.toMillis(START_UP_DELAY));
      } catch (InterruptedException e) {
        LOGGER.warn("Unexpected interrupt.");
      }
      LOGGER.info("run(): Starting apps.");
      startApps(apps);
    } catch (CommandException | KademliaException | IOException e) {
      shutdownApps(runningApps);
      return new TestResult(Type.FAILURE,
          "Could not start applications due to: " + e.getMessage() + ".");
    }

    result_ = new TestResult(Type.SUCCESS, "Connection graph was consistent the entire time.");
    ConsistencyChecker consistencyChecker = new ConsistencyChecker(apps);
    LOGGER.info("run(): Scheduling consistency checker.");
    scheduledExecutor_.scheduleWithFixedDelay(consistencyChecker, CHECK_DELAY,
        CHECK_DELAY, CHECK_DELAY_UNIT);

    synchronized (this) {
      while (!isFinished_.get()) {
        try {
          this.wait();
        } catch (InterruptedException e) {
          LOGGER.warn("Unexpected interrupt in kademlia test script.");
          Thread.currentThread().interrupt();
        }
      }
    }

    stopApps(apps);
    shutdownApps(apps);
    LOGGER.info("run() -> {}", result_);
    return result_;
  }

  private class ConsistencyChecker implements Runnable {
    private final Collection<KademliaApp> apps_;
    private int checkCount_;

    public ConsistencyChecker(Collection<KademliaApp> apps) {
      apps_ = apps;
      checkCount_ = CHECK_COUNT;
    }

    @Override
    public void run() {
      LOGGER.info("ConsistencyChecker.run()");
      Map<Key, Collection<Key>> graph;
      try {
        LOGGER.trace("ConsistencyChecker.run(): getConnectionGraph()");
        graph = getConnectionGraph(apps_);
        LOGGER.trace("ConsistencyChecker.run(): checkConsistency()");
        ConsistencyResult result = checkConsistency(graph);
        if (result.getType() == ConsistencyResult.Type.INCONSISTENT) {
          result_ = new TestResult(Type.FAILURE, "Graph is not consistent starting from: " +
            result.getStartVert() + " could only reach " + result.getReachableVerts().size() + ".");
          shutdown();
          LOGGER.info("ConsistencyChecker.run() -> failure");
          return;
        }
      } catch (IOException e) {
        result_ = new TestResult(Type.FAILURE, "Could not get connection graph: " + e + ".");
        shutdown();
        LOGGER.info("ConsistencyChecker.run() -> failure");
        return;
      }
      

      --checkCount_;
      if (checkCount_ == 0) {
        shutdown();
      }

      LOGGER.info("ConsistencyChecker.run() -> success");
    }
    
    private void shutdown() {
    	scheduledExecutor_.shutdown();
      
      synchronized (KademliaConsistencyCheckTestScript.this) {
        LOGGER.info("ConsistencyChecker.run(): Notifying that this is the last test.");
        isFinished_.set(true);
        KademliaConsistencyCheckTestScript.this.notifyAll();
      }
      
    }
  }

  private static class ConsistencyResult {
    private final Key startVert_;
    private final Collection<Key> reachableVerts_;
    private final Collection<Key> unreachableVerts_;
    
    public ConsistencyResult(Key startVert, Collection<Key> reachableVerts, 
        Collection<Key> unreachableVerts) {
      startVert_ = startVert;
      reachableVerts_ = new LinkedList<>(reachableVerts);
      unreachableVerts_ = new LinkedList<>(unreachableVerts);
    }
    
    public Collection<Key> getReachableVerts() {
      return reachableVerts_;
    }
  
    public Key getStartVert() {
      return startVert_;
    }
  
    public Collection<Key> getUnreachableVerts() {
      return unreachableVerts_;
    }
    
    public Type getType() {
      if (getUnreachableVerts().size() == 0) {
        return Type.CONSISTENT;
      } else {
        return Type.INCONSISTENT;
      }
    }
    
    public static enum Type {
      CONSISTENT,
      INCONSISTENT;
    }
  }

  private static ConsistencyResult checkConsistency(Map<Key, Collection<Key>> graph) {
    Set<Key> reachableVerts = new HashSet<Key>();
    Queue<Key> toVisit = new LinkedList<>();
    if (graph.size() == 0) {
      return new ConsistencyResult(new Key(0), new LinkedList<Key>(), new LinkedList<Key>());
    }

    Key firstVert = graph.keySet().iterator().next();
    toVisit.add(firstVert);
    reachableVerts.add(firstVert);
    
    while (!toVisit.isEmpty()) {
      Key curVert = toVisit.remove();
      for (Key vert: graph.get(curVert)) {
        if (!reachableVerts.contains(vert)) {
          toVisit.add(vert);
          reachableVerts.add(vert);
        }
      }
    }
    
    if (graph.size() == reachableVerts.size()) {
      return new ConsistencyResult(firstVert, reachableVerts, new LinkedList<Key>());
    } else {
      Set<Key> allVerts = new HashSet<Key>(graph.keySet());
      allVerts.removeAll(reachableVerts);
      return new ConsistencyResult(firstVert, reachableVerts, allVerts);
    }
  }
  
  private static Map<Key, Collection<Key>> getConnectionGraph(Collection<KademliaApp> apps) throws IOException {
    LOGGER.trace("getConnectionGraph({})", apps.size());
    Map<Key, Collection<Key>> graph = new HashMap<>();
    for (KademliaApp app: apps) {
      Key key = app.getKey();
      Collection<Key> connectedApps = new LinkedList<>();
      LOGGER.trace("getConnectionGraph(): app[{}].getRoutingTable()", app.getId());
      Collection<NodeInfo> nodeInfos = app.getRoutingTable();
      for (NodeInfo info: nodeInfos) {
        connectedApps.add(info.getKey());
      }
      graph.put(key, connectedApps);
    }

    return graph;
  }

  private static void runApps(Collection<KademliaApp> apps)
      throws CommandException {
    Collection<KademliaApp> startedApps = new LinkedList<>();
    for (KademliaApp app : apps) {
      try {
        app.run();
        startedApps.add(app);
      } catch (CommandException e) {
        LOGGER.error("runApps() -> Could not run kademlia application: {}",
            app.getName(), e);
        for (KademliaApp startApp : startedApps) {
          try {
            startApp.shutDown();
          } catch (KademliaException e1) {
            LOGGER.error(
                "runApps() -> Could not shutdown kademlia application: {}.",
                startApp.getName(), e);
          }
        }
        throw e;
      }
    }
  }

  private static void startApps(Collection<KademliaApp> apps)
      throws KademliaException, IOException {
    Collection<KademliaApp> runApps = new LinkedList<>();
    for (KademliaApp app : apps) {
      try {
        app.start();
        runApps.add(app);
      } catch (IOException | KademliaException e) {
        LOGGER.error("startApps() -> Could not start kademlia application: {}",
            app.getName(), e);
        for (KademliaApp appToStop : runApps) {
          try {
            appToStop.shutDown();
          } catch (KademliaException e1) {
            LOGGER.error(
                "startApps() -> Could not shutdown kademlia application: {}.",
                appToStop.getName(), e);
          }
        }
        throw e;
      }
    }
  }

  private static void stopApps(Collection<KademliaApp> apps) {
    for (KademliaApp app : apps) {
      try {
        app.stop();
      } catch (IOException | KademliaException e) {
        LOGGER.error("stopApps() -> Could not shutdown kademlia application: {}",
            app.getName(), e);
      }
    }
  }

  private static void shutdownApps(Collection<KademliaApp> apps) {
    for (KademliaApp app : apps) {
      try {
        app.shutDown();
      } catch (KademliaException e) {
        LOGGER.error("shutdownApps() -> Could not shutdown kademlia application: {}",
            app.getName(), e);
      }
    }
  }
}
