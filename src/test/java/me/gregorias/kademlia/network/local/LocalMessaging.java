package me.gregorias.kademlia.network.local;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import me.gregorias.kademlia.network.ByteListener;
import me.gregorias.kademlia.network.ByteListeningService;
import me.gregorias.kademlia.network.ByteResponseHandler;
import me.gregorias.kademlia.network.ByteSender;
import me.gregorias.kademlia.network.NetworkAddressDiscovery;
import me.gregorias.kademlia.network.UserGivenNetworkAddressDiscovery;

/**
 * Implementation of local messaging system.
 *
 * @author Grzegorz Milka
 */
public class LocalMessaging {
  private final ByteSender mByteSender;
  private final ReadWriteLock mRWLock;
  private final Lock mReadLock;
  private final Lock mWriteLock;
  private final Map<Integer, ByteListener> mPortToListenerMap;

  public LocalMessaging() {
    mByteSender = new ByteSenderImpl();
    mRWLock = new ReentrantReadWriteLock();
    mReadLock = mRWLock.readLock();
    mWriteLock = mRWLock.writeLock();
    mPortToListenerMap = new HashMap<Integer, ByteListener>();
  }

  public ByteListeningService getByteListeningService(int port) {
    return new ByteListeningServiceImpl(port);
  }

  public ByteSender getByteSender() {
    return mByteSender;
  }

  public NetworkAddressDiscovery getNetworkAddressDiscovery(int port) {
    return new UserGivenNetworkAddressDiscovery(new InetSocketAddress(port));
  }

  private class ByteListeningServiceImpl implements ByteListeningService {
    private final int mPort;

    public ByteListeningServiceImpl(int port) {
      mPort = port;
    }

    @Override
    public void registerListener(ByteListener listener) {
      mWriteLock.lock();
      try {
        if (mPortToListenerMap.containsKey(mPort)) {
          throw new IllegalStateException(String.format("Port: %d already has a listener", mPort));
        }
        mPortToListenerMap.put(mPort, listener);
      } finally {
        mWriteLock.unlock();
      }
    }

    @Override
    public void unregisterListener(ByteListener listener) {
      mWriteLock.lock();
      try {
        ByteListener byteListener = mPortToListenerMap.get(mPort);
        if (byteListener == null) {
          throw new IllegalStateException(String.format("Port: %d has no listener", mPort));
        } else if (byteListener != listener) {
          throw new IllegalStateException("Given listener hasn't been registered.");
        }
        mPortToListenerMap.remove(mPort);
      } finally {
        mWriteLock.unlock();
      }
    }
  }

  private class ByteSenderImpl implements ByteSender {
    @Override
    public void sendMessageWithReply(InetSocketAddress dest, byte[] array,
        ByteResponseHandler handler) {
      byte[] response;
      mReadLock.lock();
      try {
        ByteListener byteListener = mPortToListenerMap.get(dest.getPort());
        if (byteListener == null) {
          handler.onSendError(new IOException(String.format("ByteListener to port: %d hasn't"
              + " been registered.", dest.getPort())));
          return;
        } else {
          response = byteListener.receiveByteArrayWithResponse(array);
        }
      } finally {
        mReadLock.unlock();
      }
      handler.onSendSuccessful();
      if (response != null) {
        handler.onResponse(response);
      } else {
        handler.onResponseError(new IOException("Received null response"));
      }
    }
  }
}
