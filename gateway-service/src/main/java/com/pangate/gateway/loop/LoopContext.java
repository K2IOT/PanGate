package com.pangate.gateway.loop;

import com.pangate.gateway.session.Session;
import io.netty.channel.EventLoop;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/**
 * State owned by one event loop (spec §3.3): its sessions and its deadline wheel. Everything except
 * {@link #activeCount()} must be used on the owning loop only.
 */
public final class LoopContext {

  private final int index;
  private final EventLoop loop;
  private final LongSupplier monoSec;
  private final LongSupplier wallSec;
  private final DeadlineWheel wheel;
  private final Long2ObjectOpenHashMap<Session> sessions = new Long2ObjectOpenHashMap<>();
  private final com.pangate.gateway.downstream.TopicIndex topics = new com.pangate.gateway.downstream.TopicIndex();
  private long counter;
  private volatile int activeCount;
  private int pendingInflight;
  private final it.unimi.dsi.fastutil.objects.ObjectArrayList<Session> dirty =
      new it.unimi.dsi.fastutil.objects.ObjectArrayList<>();
  private volatile long outboundBytes;
  private volatile int unwritable;  // written on the loop only
  private volatile int handshaking;  // admitted, not yet activated; written on the loop only
  private static final java.lang.invoke.VarHandle PENDING;

  static {
    try {
      PENDING = java.lang.invoke.MethodHandles.lookup().findVarHandle(LoopContext.class, "pendingInflight", int.class);
    } catch (ReflectiveOperationException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  /** Same clock for deadlines and wall time (tests). */
  public LoopContext(int index, EventLoop loop, int wheelSlots, LongSupplier epochSec) {
    this(index, loop, wheelSlots, epochSec, epochSec);
  }

  /**
   * @param monoSec monotonic seconds: drive the wheel and every session deadline, so wall-clock steps (NTP, VM
   *     resume) neither close all sessions nor freeze timeouts
   * @param wallSec epoch seconds: token and ticket times only
   */
  public LoopContext(int index, EventLoop loop, int wheelSlots, LongSupplier monoSec, LongSupplier wallSec) {
    this.index = index;
    this.loop = loop;
    this.monoSec = monoSec;
    this.wallSec = wallSec;
    this.wheel = new DeadlineWheel(wheelSlots, monoSec.getAsLong());
  }

  public int index() {
    return index;
  }

  public EventLoop eventLoop() {
    return loop;
  }

  public boolean inLoop() {
    return loop.inEventLoop();
  }

  /** Monotonic seconds for deadlines. */
  public long nowSec() {
    return monoSec.getAsLong();
  }

  /** Epoch seconds for JWT and resume-ticket times. */
  public long wallSec() {
    return wallSec.getAsLong();
  }

  /** {@code (index << 40) | counter}: the loop index is recoverable from the key. */
  public long nextSessionKey() {
    return ((long) index << 40) | (++counter & 0xFF_FFFF_FFFFL);
  }

  public void register(Session s) {
    sessions.put(s.sessionKey(), s);
  }

  public void unregister(Session s) {
    sessions.remove(s.sessionKey());
  }

  public Session session(long key) {
    return sessions.get(key);
  }

  public int sessionCount() {
    return sessions.size();
  }

  public void forEachSession(Consumer<Session> action) {
    for (Session s : sessions.values().toArray(new Session[0])) {
      action.accept(s);
    }
  }

  /** Channels admitted on this loop (handshaking or active). Written on the loop, read anywhere. */
  public int activeCount() {
    return activeCount;
  }

  @SuppressWarnings("NonAtomicVolatileUpdate")
  public void incrementActive() {
    activeCount++;
  }

  @SuppressWarnings("NonAtomicVolatileUpdate")
  public void decrementActive() {
    activeCount--;
  }

  /** A session was written to: it is sampled until its outbound buffer is empty (spec §7.2). */
  public void markDirty(Session s) {
    if (!s.dirty()) {
      s.dirty(true);
      dirty.add(s);
    }
  }

  /** On the loop: sums the dirty sessions' pending bytes and drops those that are empty (or closed). */
  public void sampleOutbound() {
    long sum = 0;
    int keep = 0;
    for (int i = 0; i < dirty.size(); i++) {
      Session s = dirty.get(i);
      long b = s.pendingOutboundBytes();
      if (b > 0 && s.state() != com.pangate.gateway.session.SessionState.CLOSED) {
        sum += b;
        dirty.set(keep++, s);
      } else {
        s.dirty(false);
      }
    }
    dirty.size(keep);
    outboundBytes = sum;
  }

  /** Sessions still HANDSHAKING on this loop (spec §7.3, L3 trend); any thread. */
  public int handshaking() {
    return handshaking;
  }

  @SuppressWarnings("NonAtomicVolatileUpdate")
  public void addHandshaking(int delta) {
    handshaking += delta;
  }

  /** Channels of this loop currently unwritable; written on the loop, read anywhere. */
  public int unwritable() {
    return unwritable;
  }

  @SuppressWarnings("NonAtomicVolatileUpdate")
  public void addUnwritable(int delta) {
    unwritable += delta;
  }

  /** Last sampled outbound bytes of this loop; any thread. */
  public long outboundBytes() {
    return outboundBytes;
  }

  /** On the loop: sessions with outbound bytes at the last sample (largest first is the caller's job). */
  public java.util.List<Session> dirtySessions() {
    return java.util.Collections.unmodifiableList(dirty);
  }

  /** Pushes awaiting ACK on this loop: written on the loop, read anywhere (opaque, for the gauge). */
  public int pendingInflight() {
    return (int) PENDING.getOpaque(this);
  }

  public void addPendingInflight(int delta) {
    PENDING.setOpaque(this, pendingInflight + delta);
  }

  private com.pangate.gateway.downstream.BroadcastPacer pacer;
  private com.pangate.gateway.lifecycle.Recycler recycler;

  /** Connection recycling of this loop's sessions (spec §5.9); null = never recycle. */
  public com.pangate.gateway.lifecycle.Recycler recycler() {
    return recycler;
  }

  public void recycler(com.pangate.gateway.lifecycle.Recycler value) {
    recycler = value;
  }

  /** Broadcast pacing of this loop; attached once at wiring (PushConfig). */
  public com.pangate.gateway.downstream.BroadcastPacer pacer() {
    return pacer;
  }

  public void pacer(com.pangate.gateway.downstream.BroadcastPacer value) {
    pacer = value;
  }

  /** Topic subscriptions of this loop's sessions (spec §6.4). */
  public com.pangate.gateway.downstream.TopicIndex topics() {
    return topics;
  }

  public DeadlineWheel wheel() {
    return wheel;
  }

  /** One task per loop per second advances the wheel; no per-channel timers. */
  public void startTicker() {
    loop.scheduleAtFixedRate(() -> advance(nowSec()), 1, 1, TimeUnit.SECONDS);
  }

  /** Runs every deadline due at {@code nowSec}; called by the ticker (and directly by tests). */
  public void advance(long nowSec) {
    wheel.advanceTo(nowSec, n -> ((Deadlined) n).onDeadline(nowSec));
  }
}
