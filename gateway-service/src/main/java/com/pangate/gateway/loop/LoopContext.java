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
  private final LongSupplier epochSec;
  private final DeadlineWheel wheel;
  private final Long2ObjectOpenHashMap<Session> sessions = new Long2ObjectOpenHashMap<>();
  private long counter;
  private volatile int activeCount;

  public LoopContext(int index, EventLoop loop, int wheelSlots, LongSupplier epochSec) {
    this.index = index;
    this.loop = loop;
    this.epochSec = epochSec;
    this.wheel = new DeadlineWheel(wheelSlots, epochSec.getAsLong());
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

  public long nowSec() {
    return epochSec.getAsLong();
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

  public DeadlineWheel wheel() {
    return wheel;
  }

  /** One task per loop per second advances the wheel; no per-channel timers. */
  public void startTicker() {
    loop.scheduleAtFixedRate(() -> {
      long now = nowSec();
      wheel.advanceTo(now, n -> ((Deadlined) n).onDeadline(now));
    }, 1, 1, TimeUnit.SECONDS);
  }
}
