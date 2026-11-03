package com.pangate.gateway.server;

/** Node-wide flags read on many threads. */
public final class NodeState {

  private volatile boolean draining;
  private volatile boolean listenersBound;

  public boolean draining() {
    return draining;
  }

  public void draining(boolean value) {
    draining = value;
  }

  public boolean listenersBound() {
    return listenersBound;
  }

  public void listenersBound(boolean value) {
    listenersBound = value;
  }
}
