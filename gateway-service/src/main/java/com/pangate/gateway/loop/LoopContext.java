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
  private long counter;
  private volatile int activeCount;
  private int pendingInflight;
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

  /** Pushes awaiting ACK on this loop: written on the loop, read anywhere (opaque, for the gauge). */
  public int pendingInflight() {
    return (int) PENDING.getOpaque(this);
  }

  public void addPendingInflight(int delta) {
    PENDING.setOpaque(this, pendingInflight + delta);
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
