package com.pangate.gateway.registry;

import com.pangate.gateway.loop.LoopContext;
import com.pangate.gateway.loop.LoopRegistry;
import com.pangate.gateway.session.Session;
import com.pangate.gateway.session.SessionState;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * After a re-epoch (spec §6.1): re-registers every ACTIVE session and re-announces it (lifecycle CONNECTED) with the
 * new epoch, paced at {@code perLoopPerSecond} per loop (300 → ~5k/s per node) in 100 ms batches on each owner loop.
 * A newer resync cancels a running one.
 */
public final class RegistryResync {

  private static final long BATCH_EVERY_MS = 100;

  private final LoopRegistry loops;
  private final Consumer<Session> resyncOne;
  private final int perBatch;
  private final AtomicLong generation = new AtomicLong();
  private final AtomicInteger pending = new AtomicInteger();
  private final int[] pendingPerLoop;

  public RegistryResync(LoopRegistry loops, Consumer<Session> resyncOne, int perLoopPerSecond) {
    this.loops = loops;
    this.resyncOne = resyncOne;
    this.perBatch = Math.max(1, perLoopPerSecond / (int) (1_000 / BATCH_EVERY_MS));
    this.pendingPerLoop = new int[loops.size()];
  }

  public void resync() {
    long gen = generation.incrementAndGet();
    for (LoopContext c : loops.all()) {
      c.eventLoop().execute(() -> start(c, gen));
    }
  }

  private void start(LoopContext c, long gen) {
    if (gen != generation.get()) {
      return;
    }
    LongArrayList keys = new LongArrayList();
    c.forEachSession(s -> {
      if (s.state() == SessionState.ACTIVE) {
        keys.add(s.sessionKey());
      }
    });
    setPending(c, keys.size());
    schedule(c, gen, keys, 0);
  }

  private void schedule(LoopContext c, long gen, LongArrayList keys, int from) {
    c.eventLoop().schedule(() -> batch(c, gen, keys, from), BATCH_EVERY_MS, TimeUnit.MILLISECONDS);
  }

  private void batch(LoopContext c, long gen, LongArrayList keys, int from) {
    if (gen != generation.get()) {
      return;
    }
    int to = Math.min(keys.size(), from + perBatch);
    for (int i = from; i < to; i++) {
      Session s = c.session(keys.getLong(i));
      if (s != null && s.state() == SessionState.ACTIVE) {
        resyncOne.accept(s);
      }
    }
    setPending(c, keys.size() - to);
    if (to < keys.size()) {
      schedule(c, gen, keys, to);
    }
  }

  private void setPending(LoopContext c, int left) {
    int idx = c.index();
    pending.addAndGet(left - pendingPerLoop[idx]);
    pendingPerLoop[idx] = left;
  }

  /** Sessions still to resync across all loops. */
  public int pending() {
    return pending.get();
  }
}
