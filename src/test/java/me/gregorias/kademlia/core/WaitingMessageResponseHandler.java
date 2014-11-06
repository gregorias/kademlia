package me.gregorias.kademlia.core;

import me.gregorias.kademlia.core.Message;
import me.gregorias.kademlia.core.MessageResponseHandler;

import java.io.IOException;

final class WaitingMessageResponseHandler implements MessageResponseHandler {
  private Boolean mHasFinished;
  private IOException mIoException;

  public WaitingMessageResponseHandler() {
    mHasFinished = false;
  }

  @Override
  public synchronized void onResponse(Message response) {
    finish();
  }

  @Override
  public synchronized void onResponseError(IOException exception) {
    mIoException = exception;
    finish();
  }

  @Override
  public void onSendSuccessful() {
  }

  @Override
  public synchronized void onSendError(IOException exception) {
    mIoException = exception;
    finish();
  }

  public synchronized void waitForResponse() throws InterruptedException, IOException {
    while (!mHasFinished) {
      this.wait();
    }

    if (mIoException != null) {
      throw mIoException;
    }

    this.notifyAll();
  }

  private synchronized void finish() {
    mHasFinished = true;
    this.notifyAll();
  }

}
