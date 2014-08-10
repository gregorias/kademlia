package org.nebulostore.kademlia.interfaces.rest;

import java.net.InetSocketAddress;

import org.nebulostore.kademlia.core.Key;
import org.nebulostore.kademlia.core.NodeInfo;

public final class NodeInfoBean {
    private String key_;
    private String inetAddress_;
    private int port_;
    
    public static NodeInfoBean fromNodeInfo(NodeInfo info) {
    	NodeInfoBean bean = new NodeInfoBean();
    	bean.setKey(info.getKey().toInt().toString(Key.HEX));
    	bean.setInetAddress(info.getSocketAddress().getHostName());
    	bean.setPort(info.getSocketAddress().getPort());
    	return bean;
    }

	public String getInetAddress() {
		return inetAddress_;
	}

	public String getKey() {
		return key_;
	}

	public int getPort() {
		return port_;
	}

	public void setInetAddress(String inetAddress) {
		inetAddress_ = inetAddress;
	}

	public void setKey(String key) {
		key_ = key;
	}

	public void setPort(int port) {
		port_ = port;
	}

    public NodeInfo toNodeInfo() {
    	return new NodeInfo(new Key(key_), new InetSocketAddress(inetAddress_, port_));
    }
    
}
