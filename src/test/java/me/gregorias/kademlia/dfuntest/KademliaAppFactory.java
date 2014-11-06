package me.gregorias.kademlia.dfuntest;

import java.net.URI;
import java.net.URISyntaxException;

import me.gregorias.dfuntest.ApplicationFactory;
import me.gregorias.dfuntest.Environment;
import me.gregorias.kademlia.interfaces.Main;

/**
 * Factory of {@link KademliaApp} on given {@link Environment}.
 *
 * @author Grzegorz Milka
 */
public class KademliaAppFactory implements ApplicationFactory<KademliaApp> {
  private final String mJavaCommand;

  public KademliaAppFactory(String javaCommand) {
    mJavaCommand = javaCommand;
  }
  @Override
  public KademliaApp newApp(Environment env) {
    String address;
    int port;
    String javaCommand;
    try {
      address = (String) env.getProperty(Main.XML_FIELD_LOCAL_ADDRESS);
      port = (int) env.getProperty(Main.XML_FIELD_REST_PORT);
    } catch (ClassCastException | NullPointerException e) {
      throw new IllegalArgumentException("Environment did not contain a required field.", e);
    }
    URI uri;
    try {
      uri = new URI("http", null, address, port, null, null, null);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Could not create valid URI.", e);
    }
    return new KademliaApp(env.getId(), String.format("Local Kademlia[%d]", env.getId()),
        uri, env, mJavaCommand);
  }
}
