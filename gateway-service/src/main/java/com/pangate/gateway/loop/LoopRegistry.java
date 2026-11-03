package com.pangate.gateway.loop;

import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.EventExecutor;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;

/** Index of every {@link LoopContext}, by position and by event loop. */
public final class LoopRegistry {

  private final List<LoopContext> contexts;
  private final Map<EventExecutor, LoopContext> byLoop;

  private LoopRegistry(List<LoopContext> contexts, Map<EventExecutor, LoopContext> byLoop) {
    this.contexts = contexts;
    this.byLoop = byLoop;
  }

  public static LoopRegistry create(EventLoopGroup group, int wheelSlots, LongSupplier epochSec) {
    return create(group, wheelSlots, epochSec, epochSec);
  }

  /** @see LoopContext#LoopContext(int, EventLoop, int, LongSupplier, LongSupplier) */
  public static LoopRegistry create(EventLoopGroup group, int wheelSlots, LongSupplier monoSec,
      LongSupplier wallSec) {
    List<LoopContext> list = new ArrayList<>();
    Map<EventExecutor, LoopContext> map = new IdentityHashMap<>();
    for (EventExecutor e : group) {
      LoopContext ctx = new LoopContext(list.size(), (EventLoop) e, wheelSlots, monoSec, wallSec);
      list.add(ctx);
      map.put(e, ctx);
    }
    return new LoopRegistry(List.copyOf(list), map);
  }

  /** Registry over existing contexts (each context's own event loop is its key). */
  public static LoopRegistry fromContexts(List<LoopContext> contexts) {
    Map<EventExecutor, LoopContext> map = new IdentityHashMap<>();
    for (LoopContext c : contexts) {
      map.put(c.eventLoop(), c);
    }
    return new LoopRegistry(List.copyOf(contexts), map);
  }

  public LoopContext get(int idx) {
    return contexts.get(idx);
  }

  public LoopContext of(EventLoop loop) {
    LoopContext ctx = byLoop.get(loop);
    if (ctx == null) {
      throw new IllegalArgumentException("Event loop is not part of the gateway group: " + loop);
    }
    return ctx;
  }

  public int size() {
    return contexts.size();
  }

  public List<LoopContext> all() {
    return contexts;
  }

  public int totalActive() {
    int sum = 0;
    for (LoopContext c : contexts) {
      sum += c.activeCount();
    }
    return sum;
  }
}
