package me.gregorias.kademlia.network;

import java.net.InetSocketAddress;
import java.util.Observable;

/**
 * Objects of this class are responsible for finding out host's network IP/port
 * address and monitoring its change.
 *
 * It is observable and informs observers about change when new valid address is
 * found.
 *
 * @author Grzegorz Milka
 */
public abstract class NetworkAddressDiscovery extends Observable {
  /**
   * @return Current network address or null iff no valid address could be
   *         found.
   */
  public abstract InetSocketAddress getNetworkAddress();
}
