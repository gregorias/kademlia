package org.nebulostore.kademlia.core;

import java.io.IOException;

final class WaitingMessageResponseHandler implements
		MessageResponseHandler {
	private Boolean hasFinished_;
	private IOException ioException_;
	
	public WaitingMessageResponseHandler() {
		hasFinished_ = false;
	}


	@Override
	public synchronized void onResponse(Message response) {
		finish();
	}

	@Override
	public synchronized void onResponseError(IOException e) {
		ioException_ = e;
		finish();
	}

	@Override
	public void onSendSuccessful() {
	}

	@Override
	public synchronized void onSendError(IOException e) {
		ioException_ = e;
		finish();
	}
	
	public synchronized void waitForResponse() throws InterruptedException, IOException {
		while (!hasFinished_) {
			this.wait();
		}

		if (ioException_ != null) {
			throw ioException_;
		}

		this.notifyAll();
	}
	
	private synchronized void finish() {
		hasFinished_ = true;
		this.notifyAll();
	}

}
