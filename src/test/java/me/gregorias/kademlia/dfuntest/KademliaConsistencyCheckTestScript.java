package me.gregorias.kademlia.dfuntest;

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

import me.gregorias.dfuntest.TestResult;
import me.gregorias.dfuntest.TestScript;
import me.gregorias.dfuntest.TestResult.Type;
import me.gregorias.kademlia.core.KademliaException;
import me.gregorias.kademlia.core.Key;
import me.gregorias.kademlia.core.NodeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KademliaConsistencyCheckTestScript implements TestScript<KademliaApp> {
  private static final Logger LOGGER = LoggerFactory
      .getLogger(KademliaConsistencyCheckTestScript.class);
  private static final int START_UP_DELAY = 5;
  private static final TimeUnit START_UP_DELAY_UNIT = TimeUnit.SECONDS;
  private static final long CHECK_DELAY = 20;
  private static final TimeUnit CHECK_DELAY_UNIT = TimeUnit.SECONDS;

  /**
   * How many times consistency check should be run.
   */
  private static final int CHECK_COUNT = 5;

  private final ScheduledExecutorService mScheduledExecutor;
  private final AtomicBoolean mIsFinished;
  private TestResult mResult;

  public KademliaConsistencyCheckTestScript(ScheduledExecutorService scheduledExecutor) {
    mScheduledExecutor = scheduledExecutor;
    mIsFinished = new AtomicBoolean(false);
  }

  @Override
  public TestResult run(Collection<KademliaApp> apps) {
    LOGGER.info("run(): Running consistency test on {} hosts.", apps.size());

    try {
      startUpApps(apps);
      try {
        Thread.sleep(START_UP_DELAY_UNIT.toMillis(START_UP_DELAY));
      } catch (InterruptedException e) {
        LOGGER.warn("Unexpected interrupt.");
      }
    } catch (IOException e) {
      return new TestResult(Type.FAILURE, "Could not start up applications due to: "
        + e.getMessage() + ".");
    }

    LOGGER.info("run(): Starting apps.");
    try {
      startKademlias(apps);
    } catch (KademliaException | IOException e) {
      shutDownApps(apps);
      return new TestResult(Type.FAILURE, "Could not start kademlias due to: " + e.getMessage()
        + ".");
    }

    mResult = new TestResult(Type.SUCCESS, "Connection graph was consistent the entire time.");
    ConsistencyChecker consistencyChecker = new ConsistencyChecker(apps);
    LOGGER.info("run(): Scheduling consistency checker.");
    mScheduledExecutor.scheduleWithFixedDelay(consistencyChecker, CHECK_DELAY, CHECK_DELAY,
        CHECK_DELAY_UNIT);

    synchronized (this) {
      while (!mIsFinished.get()) {
        try {
          this.wait();
        } catch (InterruptedException e) {
          LOGGER.warn("Unexpected interrupt in kademlia test script.");
          Thread.currentThread().interrupt();
        }
      }
    }

    stopKademlias(apps);
    shutDownApps(apps);
    LOGGER.info("run() -> {}", mResult);
    return mResult;
  }

  private class ConsistencyChecker implements Runnable {
    private final Collection<KademliaApp> mApps;
    private int mCheckCount;

    public ConsistencyChecker(Collection<KademliaApp> apps) {
      mApps = apps;
      mCheckCount = CHECK_COUNT;
    }

    @Override
    public void run() {
      LOGGER.info("ConsistencyChecker.run()");
      Map<Key, Collection<Key>> graph;
      try {
        LOGGER.trace("ConsistencyChecker.run(): getConnectionGraph()");
        graph = getConnectionGraph(mApps);
        LOGGER.trace("ConsistencyChecker.run(): checkConsistency()");
        ConsistencyResult result = checkConsistency(graph);
        if (result.getType() == ConsistencyResult.Type.INCONSISTENT) {
          mResult = new TestResult(Type.FAILURE, "Graph is not consistent starting from: "
              + result.getStartVert() + " could only reach " + result.getReachableVerts().size()
              + ".");
          shutDown();
          LOGGER.info("ConsistencyChecker.run() -> failure");
          return;
        }
      } catch (IOException e) {
        mResult = new TestResult(Type.FAILURE, "Could not get connection graph: " + e + ".");
        shutDown();
        LOGGER.info("ConsistencyChecker.run() -> failure");
        return;
      }

      --mCheckCount;
      if (mCheckCount == 0) {
        shutDown();
      }

      LOGGER.info("ConsistencyChecker.run() -> success");
    }

    private void shutDown() {
      mScheduledExecutor.shutdown();

      synchronized (KademliaConsistencyCheckTestScript.this) {
        LOGGER.info("ConsistencyChecker.run(): Notifying that this is the last test.");
        mIsFinished.set(true);
        KademliaConsistencyCheckTestScript.this.notifyAll();
      }

    }
  }

  private static class ConsistencyResult {
    private final Key mStartVert;
    private final Collection<Key> mReachableVerts;
    private final Collection<Key> mUnreachableVerts;

    public ConsistencyResult(Key startVert, Collection<Key> reachableVerts,
        Collection<Key> unreachableVerts) {
      mStartVert = startVert;
      mReachableVerts = new LinkedList<>(reachableVerts);
      mUnreachableVerts = new LinkedList<>(unreachableVerts);
    }

    public Collection<Key> getReachableVerts() {
      return mReachableVerts;
    }

    public Key getStartVert() {
      return mStartVert;
    }

    public Collection<Key> getUnreachableVerts() {
      return mUnreachableVerts;
    }

    public Type getType() {
      if (getUnreachableVerts().size() == 0) {
        return Type.CONSISTENT;
      } else {
        return Type.INCONSISTENT;
      }
    }

    public static enum Type {
      CONSISTENT, INCONSISTENT;
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
      for (Key vert : graph.get(curVert)) {
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

  private static Map<Key, Collection<Key>> getConnectionGraph(Collection<KademliaApp> apps) throws
      IOException {
    LOGGER.trace("getConnectionGraph({})", apps.size());
    Map<Key, Collection<Key>> graph = new HashMap<>();
    for (KademliaApp app : apps) {
      Key key = app.getKey();
      Collection<Key> connectedApps = new LinkedList<>();
      LOGGER.trace("getConnectionGraph(): app[{}].getRoutingTable()", app.getId());
      Collection<NodeInfo> nodeInfos = app.getRoutingTable();
      for (NodeInfo info : nodeInfos) {
        connectedApps.add(info.getKey());
      }
      graph.put(key, connectedApps);
    }

    return graph;
  }

  private static void startUpApps(Collection<KademliaApp> apps) throws IOException {
    Collection<KademliaApp> startedApps = new LinkedList<>();
    for (KademliaApp app : apps) {
      try {
        app.startUp();
        startedApps.add(app);
      } catch (IOException e) {
        LOGGER.error("runApps() -> Could not run kademlia application: {}", app.getName(), e);
        for (KademliaApp startApp : startedApps) {
          try {
            startApp.shutDown();
          } catch (IOException e1) {
            LOGGER.error("runApps() -> Could not shutdown kademlia application: {}.",
                startApp.getName(), e);
          }
        }
        throw e;
      }
    }
  }

  private static void startKademlias(Collection<KademliaApp> kademlias) throws
      KademliaException,
      IOException {
    Collection<KademliaApp> runApps = new LinkedList<>();
    for (KademliaApp kademlia : kademlias) {
      try {
        kademlia.start();
        runApps.add(kademlia);
      } catch (IOException | KademliaException e) {
        LOGGER.error("startApps() -> Could not start kademlia application: {}",
            kademlia.getName(), e);
        for (KademliaApp appToStop : runApps) {
          try {
            appToStop.shutDown();
          } catch (IOException e1) {
            LOGGER.error("startApps() -> Could not shutdown kademlia application: {}.",
                appToStop.getName(), e);
          }
        }
        throw e;
      }
    }
  }

  private static void stopKademlias(Collection<KademliaApp> kademlias) {
    for (KademliaApp kademlia : kademlias) {
      try {
        kademlia.stop();
      } catch (IOException | KademliaException e) {
        LOGGER.error("stopApps() -> Could not shutdown kademlia application: {}",
            kademlia.getName(), e);
      }
    }
  }

  private static void shutDownApps(Collection<KademliaApp> apps) {
    for (KademliaApp app : apps) {
      try {
        app.shutDown();
      } catch (IOException e) {
        LOGGER.error("shutdownApps() -> Could not shutdown kademlia application: {}",
            app.getName(), e);
      }
    }
  }
}
