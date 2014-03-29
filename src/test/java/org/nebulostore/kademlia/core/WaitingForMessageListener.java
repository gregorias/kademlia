package org.nebulostore.kademlia.core;

public class WaitingForMessageListener implements MessageListener {
	private Message msg_;
	private boolean isWaiting_ = false;

	public synchronized void initializeCatchingTheNextMessage() {
		isWaiting_ = true;
	}
	
	@Override
	public FindNodeReplyMessage receiveFindNodeMessage(FindNodeMessage msg) {
		checkWaiting(msg);
		return null;
	}

	@Override
	public PongMessage receiveGetKeyMessage(GetKeyMessage msg) {
		checkWaiting(msg);
		return null;
	}

	@Override
	public synchronized PongMessage receivePingMessage(PingMessage msg) {
		checkWaiting(msg);
		return null;
	}

	public synchronized Message waitForMessage() throws InterruptedException {
		if (!isWaiting_ && msg_ == null) {
			throw new IllegalStateException("Listener is not waiting for ping.");
		}
		while (msg_ == null) {
			this.wait();
		}
		Message msg = msg_;
		msg_ = null;
		return msg;
	}

	private synchronized void checkWaiting(Message msg) {
		if (isWaiting_) {
			msg_ = msg;
			isWaiting_ = false;
			this.notifyAll();
		}
	}

}
