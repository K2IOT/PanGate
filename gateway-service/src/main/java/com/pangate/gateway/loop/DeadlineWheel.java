package com.pangate.gateway.loop;

import java.util.function.Consumer;

/**
 * Per-event-loop hashed wheel with 1 s ticks (spec §3.5). Each node sits in the slot of its deadline; nodes whose
 * deadline is further than one revolution stay in place until a visit finds them due (lazy rebucketing).
 * Not thread-safe: used only by the owning event loop.
 */
public final class DeadlineWheel {

  private final WheelNode[] heads;
  private long lastTick;
  private int size;

  public DeadlineWheel(int slots, long startSec) {
    this.heads = new WheelNode[slots];
    this.lastTick = startSec;
  }

  public void schedule(WheelNode n, long deadlineSec) {
    remove(n);
    n.deadlineSec = deadlineSec;
    long bucket = Math.max(deadlineSec, lastTick + 1);
    int slot = (int) Math.floorMod(bucket, (long) heads.length);
    n.slot = slot;
    n.prev = null;
    n.next = heads[slot];
    if (heads[slot] != null) {
      heads[slot].prev = n;
    }
    heads[slot] = n;
    size++;
  }

  public void remove(WheelNode n) {
    if (n.slot < 0) {
      return;
    }
    if (n.prev != null) {
      n.prev.next = n.next;
    } else {
      heads[n.slot] = n.next;
    }
    if (n.next != null) {
      n.next.prev = n.prev;
    }
    n.prev = null;
    n.next = null;
    n.slot = -1;
    size--;
  }

  /** Visits every second from the last tick to {@code nowSec}; due nodes are removed before {@code onDue} runs. */
  public void advanceTo(long nowSec, Consumer<WheelNode> onDue) {
    if (nowSec <= lastTick) {
      return;
    }
    long from = lastTick + 1;
    long steps = Math.min(nowSec - lastTick, heads.length);
    long start = nowSec - lastTick > heads.length ? nowSec - heads.length + 1 : from;
    for (long i = 0; i < steps; i++) {
      long t = start + i;
      lastTick = t;
      int slot = (int) Math.floorMod(t, (long) heads.length);
      WheelNode n = heads[slot];
      while (n != null) {
        WheelNode next = n.next;
        if (n.deadlineSec <= nowSec) {
          remove(n);
          onDue.accept(n);
        }
        n = next;
      }
    }
    lastTick = nowSec;
  }

  public int size() {
    return size;
  }
}
