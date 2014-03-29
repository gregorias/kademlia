package org.nebulostore.kademlia.network;

import java.net.InetSocketAddress;

/**
 * {@link NetworkAddressDiscovery} which always returns the same address given at initialization.
 * 
 * @author Grzegorz Milka
 *
 */
public final class UserGivenNetworkAddressDiscovery extends NetworkAddressDiscovery {
	private final InetSocketAddress address_;

	public UserGivenNetworkAddressDiscovery(InetSocketAddress address) {
		address_ = address;
	}

	@Override
	public InetSocketAddress getNetworkAddress() {
		return address_;
	}
}