package com.pangate.gateway.loop;

/** Intrusive entry of a {@link DeadlineWheel}: no per-entry allocation, O(1) removal. Owned by one event loop. */
public abstract class WheelNode {

  long deadlineSec;
  WheelNode prev;
  WheelNode next;
  int slot = -1;

  public final boolean isScheduled() {
    return slot >= 0;
  }

  public final long scheduledDeadlineSec() {
    return deadlineSec;
  }
}
