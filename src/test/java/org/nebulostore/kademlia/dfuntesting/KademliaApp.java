package org.nebulostore.kademlia.dfuntesting;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status.Family;

import org.nebulostore.dfuntesting.App;
import org.nebulostore.dfuntesting.Environment;
import org.nebulostore.dfuntesting.RemoteProcess;
import org.nebulostore.kademlia.core.KademliaException;
import org.nebulostore.kademlia.core.Key;
import org.nebulostore.kademlia.core.NodeInfo;
import org.nebulostore.kademlia.interfaces.rest.NodeInfoBean;
import org.nebulostore.kademlia.interfaces.rest.NodeInfoCollectionBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link App} to Kademlia application which uses REST interface for
 * communication.
 *
 * KademliaApp assumes that:
 * <ul>
 * <li> Simple logger is used and will output logs into stderr.log file.</li>
 * <li> All dependencies are in lib/ directory.</li>
 * </ul>
 *
 * @author Grzegorz Milka
 */
public class KademliaApp extends App {
  public static final String LOG_FILE = "stderr.log";
  private static final Logger LOGGER = LoggerFactory.getLogger(KademliaApp.class);
  private final Environment mKademliaEnv;
  private final URI mUri;
  private final String mJavaCommand;
  private RemoteProcess mProcess;

  public KademliaApp(int id, String name, URI uri, Environment env, String javaCommand) {
    super(id, name);
    mUri = uri;
    mKademliaEnv = env;
    mJavaCommand = javaCommand;
  }

  public Collection<NodeInfo> findNodes(Key key) throws IOException {
    Client client = ClientBuilder.newClient();
    WebTarget target = client.target(mUri).path("find_nodes/" + key.toInt().toString(Key.HEX));

    NodeInfoCollectionBean beanColl;
    try {
      beanColl = target.request(MediaType.APPLICATION_JSON_TYPE).get(NodeInfoCollectionBean.class);
    } catch (ProcessingException e) {
      throw new IOException("Could not find node.", e);
    }
    NodeInfoBean[] beans = beanColl.getNodeInfo();
    Collection<NodeInfo> infos = new LinkedList<>();
    for (NodeInfoBean bean : beans) {
      infos.add(bean.toNodeInfo());
    }
    return infos;
  }

  public Key getKey() throws IOException {
    LOGGER.debug("getKey()");
    Client client = ClientBuilder.newClient();
    WebTarget target = client.target(mUri).path("get_key");

    String keyStr;
    try {
      keyStr = target.request(MediaType.TEXT_PLAIN_TYPE).get(String.class);
    } catch (ProcessingException e) {
      LOGGER.error("getKey()", e);
      throw new IOException("Could not get key.", e);
    }
    LOGGER.debug("getKey() -> {}", keyStr);
    return new Key(keyStr);
  }

  public Collection<NodeInfo> getRoutingTable() throws IOException {
    LOGGER.debug("getRoutingTable()");
    Client client = ClientBuilder.newClient();
    WebTarget target = client.target(mUri).path("get_routing_table");

    NodeInfoCollectionBean beanColl;
    try {
      beanColl = target.request(MediaType.APPLICATION_JSON_TYPE).get(NodeInfoCollectionBean.class);
    } catch (ProcessingException e) {
      LOGGER.error("getRoutingTable()", e);
      throw new IOException("Could not get routing table", e);
    }
    NodeInfoBean[] beans = beanColl.getNodeInfo();
    Collection<NodeInfo> infos = new LinkedList<>();
    for (NodeInfoBean bean : beans) {
      infos.add(bean.toNodeInfo());
    }
    LOGGER.debug("getRoutingTable() -> returning with infos.");
    return infos;
  }

  @Override
  public synchronized boolean isRunning() {
    return mProcess != null;
  }

  public boolean isWorking() {
    try {
      getRoutingTable();
      return true;
    } catch (IOException e) {
      return false;
    }
  }

  @Override
  public synchronized void startUp() throws IOException {
    List<String> runCommand = new LinkedList<>();

    runCommand.add(mJavaCommand);
    runCommand.add("-Dorg.slf4j.simpleLogger.logFile=" + LOG_FILE);
    runCommand.add("-Dorg.slf4j.simpleLogger.defaultLogLevel=trace");
    runCommand.add("-cp");
    runCommand.add("lib/*:kademlia.jar");
    runCommand.add("org.nebulostore.kademlia.interfaces.Main");
    runCommand.add("kademlia.xml");
    mProcess = mKademliaEnv.runCommandAsynchronously(runCommand);
  }

  @Override
  public synchronized void shutDown() throws IOException {
    if (!isRunning() || mProcess == null) {
      throw new IllegalStateException("Kademlia is not running.");
    }
    Response response = sendRequest("shut_down", HTTPType.POST);
    if (!response.getStatusInfo().getFamily().equals(Family.SUCCESSFUL)) {
      LOGGER.error("Could not shut down kademlia interface: {}", response.toString());
    }
    try {
      mProcess.waitFor();
    } catch (InterruptedException e) {
      LOGGER.error("shutDown(): Received unexpected interrupt exception.", e);
      Thread.currentThread().interrupt();
    }
  }

  public void start() throws IOException, KademliaException {
    LOGGER.debug("[{} {}] start()", getId(), mUri);
    Response response;
    try {
      response = sendRequest("start", HTTPType.POST);
    } catch (ProcessingException e) {
      LOGGER.error("[{}] start() -> POST /start has failed.", e);
      throw new IOException("Could not start kademlia.", e);
    }

    if (!response.getStatusInfo().getFamily().equals(Family.SUCCESSFUL)) {
      LOGGER.error("[{}] start() -> POST /start has failed.");
      throw new KademliaException(
          String.format("Could not start kademlia: %s", response.toString()));
    }
  }

  public void stop() throws IOException, KademliaException {
    Response response;
    try {
      response = sendRequest("stop", HTTPType.POST);
    } catch (ProcessingException e) {
      throw new IOException("Could not stop kademlia.", e);
    }

    if (!response.getStatusInfo().getFamily().equals(Family.SUCCESSFUL)) {
      throw new KademliaException(String.format("Could not stop kademlia: %s",
          response.toString()));
    }
  }

  private enum HTTPType {
    GET, POST
  }

  private Response sendRequest(String path, HTTPType type) {
    Client client = ClientBuilder.newClient();
    WebTarget target = client.target(mUri).path(path);

    Response response;
    if (type == HTTPType.GET) {
      response = target.request().get();
    } else {
      response = target.request().post(null);
    }

    return response;
  }
}
