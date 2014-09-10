package org.nebulostore.kademlia.network;

import java.net.InetSocketAddress;

/**
 * {@link NetworkAddressDiscovery} which always returns the same address given
 * at initialization.
 *
 * @author Grzegorz Milka
 *
 */
public final class UserGivenNetworkAddressDiscovery extends NetworkAddressDiscovery {
  private final InetSocketAddress mAddress;

  public UserGivenNetworkAddressDiscovery(InetSocketAddress address) {
    mAddress = address;
  }

  @Override
  public InetSocketAddress getNetworkAddress() {
    return mAddress;
  }
}
