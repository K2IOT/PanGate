package com.pangate.gateway.server;

/** Node-wide flags read on many threads. */
public final class NodeState {

  private volatile boolean draining;
  private volatile boolean listenersBound;
  private volatile String nodeId = "";
  private volatile long epoch;
  private volatile boolean leaseAlive;
  private volatile boolean controlCaughtUp;

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

  public String nodeId() {
    return nodeId;
  }

  public void nodeId(String value) {
    nodeId = value;
  }

  /** Incarnation number from {@code INCR node:epoch:{nodeId}} (spec §6.1); 0 until the lease is taken. */
  public long epoch() {
    return epoch;
  }

  public void epoch(long value) {
    epoch = value;
  }

  public boolean leaseAlive() {
    return leaseAlive;
  }

  public void leaseAlive(boolean value) {
    leaseAlive = value;
  }

  public boolean controlCaughtUp() {
    return controlCaughtUp;
  }

  public void controlCaughtUp(boolean value) {
    controlCaughtUp = value;
  }
}
