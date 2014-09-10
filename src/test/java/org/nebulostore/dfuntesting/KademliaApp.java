package org.nebulostore.dfuntesting;

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

import org.nebulostore.kademlia.core.KademliaException;
import org.nebulostore.kademlia.core.Key;
import org.nebulostore.kademlia.core.NodeInfo;
import org.nebulostore.kademlia.interfaces.rest.NodeInfoBean;
import org.nebulostore.kademlia.interfaces.rest.NodeInfoCollectionBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KademliaApp extends App {
  private static final Logger LOGGER = LoggerFactory.getLogger(KademliaApp.class);
  private final Environment kademliaEnv_;
  private final URI uri_;
  private Process p_;

  public KademliaApp(int id, String name, URI uri, Environment env) {
    super(id, name);
    uri_ = uri;
    kademliaEnv_ = env;
  }

  @Override
  public synchronized boolean isRunning() {
    return p_ != null;
  }

  public boolean isWorking() {
    try {
      getRoutingTable();
      return true;
    } catch (IOException e) {
      return false;
    }
  }

  public Collection<NodeInfo> findNodes(Key key) throws IOException {
    Client client = ClientBuilder.newClient();
    WebTarget target = client.target(uri_).path("find_nodes/" + key.toInt().toString(Key.HEX));

    NodeInfoCollectionBean beanColl;
    try {
      beanColl = target.request(MediaType.APPLICATION_JSON_TYPE).get(NodeInfoCollectionBean.class);
    } catch (ProcessingException e) {
      throw new IOException("Could not find node.", e);
    }
    NodeInfoBean[] beans = beanColl.getNodeInfo();
    Collection<NodeInfo> infos = new LinkedList<>();
    for (NodeInfoBean bean: beans) {
      infos.add(bean.toNodeInfo());
    }
    return infos;
  }

  public Key getKey() throws IOException {
    LOGGER.debug("getKey()");
    Client client = ClientBuilder.newClient();
    WebTarget target = client.target(uri_).path("get_key");

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
    WebTarget target = client.target(uri_).path("get_routing_table");

    NodeInfoCollectionBean beanColl;
    try {
      beanColl = target.request(MediaType.APPLICATION_JSON_TYPE).get(NodeInfoCollectionBean.class);
    } catch (ProcessingException e) {
      LOGGER.error("getRoutingTable()", e);
      throw new IOException("Could not get routing table", e);
    }
    NodeInfoBean[] beans = beanColl.getNodeInfo();
    Collection<NodeInfo> infos = new LinkedList<>();
    for (NodeInfoBean bean: beans) {
      infos.add(bean.toNodeInfo());
    }
    LOGGER.debug("getRoutingTable() -> returning with infos.");
    return infos;
  }

  @Override
  public synchronized void startUp() throws CommandException {
    List<String> runCommand = new LinkedList<>();
    runCommand.add("java");
    runCommand.add("-Dorg.slf4j.simpleLogger.logFile=stderr.log");
    runCommand.add("-Dorg.slf4j.simpleLogger.defaultLogLevel=trace");
    runCommand.add("-cp");
    runCommand.add("allLibs/*:kademlia.jar");
    runCommand.add("org.nebulostore.kademlia.interfaces.Main");
    runCommand.add("kademlia.xml");
    p_ = kademliaEnv_.runCommandAsynchronously(runCommand);
  }

@Override
  public synchronized void shutDown() throws IOException {
    if (!isRunning() || p_ == null) {
      throw new IllegalStateException("Kademlia is not running.");
    }
    Response response = sendRequest("shut_down", HTTPType.POST);
    if(!response.getStatusInfo().getFamily().equals(Family.SUCCESSFUL)) {
      LOGGER.error("Could not shut down kademlia interface: {}", response.toString());
    }
    try {
      p_.waitFor();
    } catch (InterruptedException e) {
      LOGGER.error("shutDown(): Received unexpected interrupt exception.", e);
      Thread.currentThread().interrupt();
    }
  }

  public void start() throws IOException, KademliaException {
    LOGGER.debug("[{} {}] start()", getId(), uri_);
    Response response;
    try {
      response = sendRequest("start", HTTPType.POST);
    } catch (ProcessingException e) {
      LOGGER.error("[{}] start() -> POST /start has failed.", e);
      throw new IOException("Could not start kademlia." , e);
    }

    if(!response.getStatusInfo().getFamily().equals(Family.SUCCESSFUL)) {
      LOGGER.error("[{}] start() -> POST /start has failed.");
      throw new KademliaException(String.format(
          "Could not start kademlia: %s", response.toString()));
    }
  }

  public void stop() throws IOException, KademliaException {
    Response response;
    try {
      response = sendRequest("stop", HTTPType.POST);
    } catch (ProcessingException e) {
      throw new IOException("Could not stop kademlia." , e);
    }

    if(!response.getStatusInfo().getFamily().equals(Family.SUCCESSFUL)) {
      throw new KademliaException(String.format(
          "Could not stop kademlia: %s", response.toString()));
    }
  }

  private enum HTTPType {
    GET,
    POST
  }

  private Response sendRequest(String path, HTTPType type) {
    Client client = ClientBuilder.newClient();
    WebTarget target = client.target(uri_).path(path);

    Response response;
    if (type == HTTPType.GET) {
      response = target.request().get();
    } else {
      response = target.request().post(null);
    }

    return response;
  }
}
