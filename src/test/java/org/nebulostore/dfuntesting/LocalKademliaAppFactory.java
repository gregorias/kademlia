package org.nebulostore.dfuntesting;

import java.net.URI;
import java.net.URISyntaxException;

import org.nebulostore.kademlia.interfaces.Main;

public class LocalKademliaAppFactory implements ApplicationFactory<KademliaApp> {
  @Override
  public KademliaApp newApp(Environment env) {
    String address;
    int port;
    try {
      address = (String) env.getProperty(Main.XML_FIELD_LOCAL_ADDRESS);
      port = (int) env.getProperty(Main.XML_FIELD_REST_PORT);
    } catch (ClassCastException | NullPointerException e) {
      throw new IllegalArgumentException("Environment did not contain address of host.", e);
    }
    URI uri;
    try {
      uri = new URI("http", null, address, port, null, null, null);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Could not create valid URI.", e);
    }
    return new KademliaApp(env.getId(), String.format("Local Kademlia[%d]", env.getId()),
        (URI) uri, env);
  }
}
