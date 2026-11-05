package com.pangate.gateway.lifecycle;

import com.pangate.gateway.config.GatewayProperties;
import com.pangate.gateway.loop.LoopContext;
import com.pangate.gateway.loop.LoopRegistry;
import com.pangate.gateway.metrics.GatewayMetrics;
import com.pangate.gateway.protocol.CloseCode;
import com.pangate.gateway.server.NodeState;
import com.pangate.gateway.session.Session;
import com.pangate.gateway.session.SessionState;
import io.micrometer.core.instrument.Counter;
import io.netty.util.concurrent.ScheduledFuture;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Staged drain (spec §7.5), started by SIGTERM (NettyGatewayServer.stop) or POST /admin/drain; idempotent.
 * <ol>
 *   <li>draining = true (/ready 503, new handshakes refused), {@code node:draining:{id}} in Redis, listeners closed
 *       after {@code listenerCloseDelay};</li>
 *   <li>every {@code interval}, each loop sends RECONNECT{delay ∈ [0, maxDelay]} to
 *       {@code ceil(batchPercent × activeAtStart / 100)} ACTIVE sessions (they close 1001 after delay + 30 s);</li>
 *   <li>done when no session is left, or at {@code maxDuration} the rest is closed 1001.</li>
 * </ol>
 * Each loop runs its own two timers only while draining; all session work stays on the owning loop.
 */
public final class DrainCoordinator {

  private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(DrainCoordinator.class);

  private final GatewayProperties.Drain cfg;
  private final LoopRegistry loops;
  private final NodeState node;
  private final Runnable markDrainingInRedis;
  private final ScheduledExecutorService scheduler;
  private final Counter reconnects;
  private final java.util.function.LongUnaryOperator delayMs;
  private final AtomicReference<CompletableFuture<Void>> started = new AtomicReference<>();

  /**
   * @param delayMs maps {@code drain.max-delay} (ms) to one session's RECONNECT delay in [0, bound]; called on the
   *     session's event loop, so it must be thread-safe (production: {@code ThreadLocalRandom}).
   */
  public DrainCoordinator(GatewayProperties.Drain cfg, LoopRegistry loops, NodeState node,
      Runnable markDrainingInRedis, ScheduledExecutorService scheduler, GatewayMetrics m,
      java.util.function.LongUnaryOperator delayMs) {
    this.cfg = cfg;
    this.loops = loops;
    this.node = node;
    this.markDrainingInRedis = markDrainingInRedis;
    this.scheduler = scheduler;
    this.reconnects = m.counter("gw.drain.reconnects");
    this.delayMs = delayMs;
  }

  public CompletableFuture<Void> drain(Runnable closeListeners) {
    CompletableFuture<Void> done = new CompletableFuture<>();
    if (!started.compareAndSet(null, done)) {
      return started.get();
    }
    log.info("drain started: {} sessions", loops.totalActive());
    node.draining(true);
    try {
      markDrainingInRedis.run();
    } catch (RuntimeException e) {
      log.warn("could not mark the node draining in Redis: {}", e.toString());
    }
    if (cfg.listenerCloseDelay().isZero()) {
      closeListeners.run();
    } else {
      scheduler.schedule(closeListeners, cfg.listenerCloseDelay().toMillis(), TimeUnit.MILLISECONDS);
    }
    List<CompletableFuture<Void>> perLoop = new ArrayList<>();
    for (LoopContext c : loops.all()) {
      CompletableFuture<Void> loopDone = new CompletableFuture<>();
      perLoop.add(loopDone);
      c.eventLoop().execute(() -> new LoopDrain(c, loopDone).start());
    }
    CompletableFuture.allOf(perLoop.toArray(new CompletableFuture[0])).whenComplete((v, e) -> {
      log.info("drain finished");
      done.complete(null);
    });
    return done;
  }

  /** One loop's drain; every method runs on that loop. */
  private final class LoopDrain {
    private final LoopContext loop;
    private final CompletableFuture<Void> done;
    private int batch;
    private ScheduledFuture<?> interval;
    private ScheduledFuture<?> timeout;
    private int checks;

    LoopDrain(LoopContext loop, CompletableFuture<Void> done) {
      this.loop = loop;
      this.done = done;
    }

    void start() {
      int atStart = loop.activeCount();
      if (atStart == 0) {
        done.complete(null);
        return;
      }
      batch = Math.max(1, (int) Math.ceil(cfg.batchPercent() * atStart / 100.0));
      long every = cfg.interval().toMillis();
      interval = loop.eventLoop().scheduleAtFixedRate(this::tick, every, every, TimeUnit.MILLISECONDS);
      timeout = loop.eventLoop().schedule(this::forceClose, cfg.maxDuration().toMillis(), TimeUnit.MILLISECONDS);
      tick();
    }

    void tick() {
      if (loop.activeCount() == 0) {
        finish();
        return;
      }
      List<Session> picked = new ArrayList<>(batch);
      loop.forEachSession(s -> {
        if (picked.size() < batch && s.state() == SessionState.ACTIVE) {
          picked.add(s);
        }
      });
      for (Session s : picked) {
        s.drain(delayMs.applyAsLong(cfg.maxDelay().toMillis()), false);
        reconnects.increment();
      }
    }

    void forceClose() {
      interval.cancel(false);
      loop.forEachSession(s -> s.close(CloseCode.GOING_AWAY, "DRAIN_TIMEOUT"));
      awaitEmpty();
    }

    /** Closes complete asynchronously: check every 100 ms, give up after 5 s. */
    void awaitEmpty() {
      if (loop.activeCount() == 0 || ++checks > 50) {
        finish();
      } else {
        loop.eventLoop().schedule(this::awaitEmpty, 100, TimeUnit.MILLISECONDS);
      }
    }

    void finish() {
      if (interval != null) {
        interval.cancel(false);
      }
      if (timeout != null) {
        timeout.cancel(false);
      }
      done.complete(null);
    }
  }
}
