package me.gregorias.kademlia.core;

import java.util.Collection;

/**
 * Peer in Kademlia's network.
 *
 * @author Grzegorz Milka
 */
public interface KademliaRouting {
  /**
   * findClosestNodes with size equal to bucket size parameter.
   *
   * @see KademliaRouting#findClosestNodes
   */
  Collection<NodeInfo> findClosestNodes(Key key) throws InterruptedException, KademliaException;

  /**
   * Find size number of nodes closest to given {@link Key}.
   *
   * @param key
   *          key to look up
   * @param size
   *          number of nodes to find
   * @return up to size found nodes
   * @throws InterruptedException
   * @throws KademliaException
   */
  Collection<NodeInfo> findClosestNodes(Key key, int size) throws InterruptedException,
      KademliaException;

  /**
   * @return Key representing this peer
   */
  Key getLocalKey();

  /**
   * @return hosts represented in local routing table.
   */
  Collection<NodeInfo> getRoutingTable();

  /**
   * @return is kademlia running.
   */
  boolean isRunning();

  /**
   * Connect and initialize this peer.
   *
   * @throws KademliaException
   */
  void start() throws KademliaException;

  /**
   * Disconnects peer from network.
   *
   * @throws KademliaException
   */
  void stop() throws KademliaException;
}
