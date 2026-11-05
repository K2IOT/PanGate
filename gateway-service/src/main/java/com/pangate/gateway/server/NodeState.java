package com.pangate.gateway.server;

/** Node-wide flags read on many threads. */
public final class NodeState {

  private volatile boolean draining;
  private volatile boolean listenersBound;
  private volatile String nodeId = "";
  private volatile long epoch;
  private volatile boolean leaseAlive;
  private volatile boolean controlCaughtUp;
  private volatile int shedLevel;
  private volatile boolean warmedUp;

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

  /** Load-shedding level 0–6 (spec §7.3); written by the shed ladder (M3), read on the push path. */
  public int shedLevel() {
    return shedLevel;
  }

  public void shedLevel(int value) {
    shedLevel = value;
  }

  /** Warm-up finished (or skipped); readiness waits for it (spec §7.6). */
  public boolean warmedUp() {
    return warmedUp;
  }

  public void warmedUp(boolean value) {
    warmedUp = value;
  }
}
