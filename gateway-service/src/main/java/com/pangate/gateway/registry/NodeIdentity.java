package com.pangate.gateway.registry;

import com.pangate.gateway.lifecycle.ProcessExit;
import com.pangate.gateway.lifecycle.RedisConnections;
import com.pangate.gateway.metrics.GatewayMetrics;
import com.pangate.gateway.server.NodeState;
import com.pangate.gateway.server.TickScheduler;
import io.lettuce.core.ScriptOutputType;
import io.micrometer.core.instrument.Counter;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

/**
 * Node epoch and lease (spec §6.1). A fresh epoch from {@code INCR node:epoch:{id}} always beats older incarnations
 * ("higher epoch wins"), so a fast restart is never fenced, while a stale process that sees a higher epoch fences
 * itself. Renewal runs on gw-tick every 5 s and is fully async. After 15 s without a successful renewal the lease
 * counts as lost; the first renewal that reaches Redis again takes a new epoch and notifies re-epoch listeners
 * (registry resync).
 */
public final class NodeIdentity implements SmartLifecycle {

  private static final Logger log = LoggerFactory.getLogger(NodeIdentity.class);
  public static final int PHASE = Integer.MAX_VALUE - 1800;
  static final int LEASE_TTL_SEC = 15;
  static final Duration RENEW_EVERY = Duration.ofSeconds(5);

  private static final LuaScript LEASE = new LuaScript("""
      local cur = tonumber(redis.call('GET', KEYS[1]) or '0')
      if cur > tonumber(ARGV[1]) then return -1 end
      redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
      return 1
      """, ScriptOutputType.INTEGER);
  private static final LuaScript RELEASE = new LuaScript("""
      if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end
      return 0
      """, ScriptOutputType.INTEGER);

  private final String nodeId;
  private final RedisConnections redis;
  private final NodeState node;
  private final TickScheduler tick;
  private final LongSupplier monoMillis;
  private final LongSupplier wallMillis;
  private final ProcessExit exit;
  private final Counter renewFailures;
  private final List<LongConsumer> reEpochListeners = new CopyOnWriteArrayList<>();
  private final AtomicBoolean fenced = new AtomicBoolean();
  private final AtomicBoolean renewing = new AtomicBoolean();
  private volatile long epoch;
  private volatile long lastOkMono;
  private volatile boolean lost;
  private volatile boolean running;
  private ScheduledFuture<?> renewTask;

  public NodeIdentity(String nodeId, RedisConnections redis, NodeState node, TickScheduler tick,
      LongSupplier monoMillis, LongSupplier wallMillis, ProcessExit exit, GatewayMetrics metrics) {
    this.nodeId = nodeId;
    this.redis = redis;
    this.node = node;
    this.tick = tick;
    this.monoMillis = monoMillis;
    this.wallMillis = wallMillis;
    this.exit = exit;
    this.renewFailures = metrics.counter("gw.lease.renew.failures");
    metrics.gauge("gw.node.epoch", node, NodeState::epoch);
  }

  /** Blocking (≤ 5 s per call): runs on the Spring lifecycle thread, after InfraLifecycle connected Redis. */
  @Override
  public void start() {
    try {
      long e = redis.registry().incr(RegistryKeys.epoch(nodeId)).get(5, TimeUnit.SECONDS);
      Long r = LEASE.<Long>run(redis.registry(), RegistryKeys.node(nodeId), Long.toString(e),
          Integer.toString(LEASE_TTL_SEC)).toCompletableFuture().get(5, TimeUnit.SECONDS);
      if (r == null || r != 1L) {
        throw new IllegalStateException("node " + nodeId + " lease refused for fresh epoch " + e);
      }
      redis.registry().zadd(RegistryKeys.NODES_LIVE, wallMillis.getAsLong(), nodeId).get(5, TimeUnit.SECONDS);
      acquired(e);
    } catch (Exception ex) {
      throw new IllegalStateException("cannot take node lease for " + nodeId, ex);
    }
    renewTask = tick.scheduleAtFixedRate(this::renew, RENEW_EVERY);
    running = true;
    log.info("node {} holds lease with epoch {}", nodeId, epoch);
  }

  private void acquired(long e) {
    epoch = e;
    node.nodeId(nodeId);
    node.epoch(e);
    lastOkMono = monoMillis.getAsLong();
    lost = false;
    node.leaseAlive(true);
  }

  /** One renewal (gw-tick); the returned stage completes when Redis answered or failed. */
  public CompletionStage<Void> renew() {
    if (fenced.get() || !renewing.compareAndSet(false, true)) {
      return CompletableFuture.completedFuture(null);
    }
    if (!lost && monoMillis.getAsLong() - lastOkMono > LEASE_TTL_SEC * 1000L) {
      lost = true;
      node.leaseAlive(false);
      log.error("node {} lease lost: no successful renewal for {} s", nodeId, LEASE_TTL_SEC);
    }
    CompletionStage<Void> op;
    try {
      op = lost ? reEpoch().thenApply(e -> null) : renewOnce();
    } catch (RuntimeException e) {
      op = CompletableFuture.failedFuture(e);
    }
    return op.handle((v, e) -> {
      if (e != null) {
        renewFailures.increment();
        log.warn("node {} lease renewal failed: {}", nodeId, e.toString());
      }
      renewing.set(false);
      return null;
    });
  }

  private CompletionStage<Void> renewOnce() {
    long e = epoch;
    return lease(e).thenCompose(r -> {
      if (r == -1L) {
        fence(e);
        return CompletableFuture.completedFuture(null);
      }
      lastOkMono = monoMillis.getAsLong();
      return redis.registry().zadd(RegistryKeys.NODES_LIVE, wallMillis.getAsLong(), nodeId).thenApply(x -> null);
    });
  }

  /**
   * After a lost lease: a new incarnation number, then the lease, then listeners (resync). If a newer incarnation
   * took the lease meanwhile (this process was stalled past the TTL), this one fences instead (spec §6.1).
   */
  public CompletionStage<Long> reEpoch() {
    long own = epoch;
    return redis.registry().get(RegistryKeys.node(nodeId)).thenCompose(holder -> {
      long current = holder == null ? 0 : Long.parseLong(holder);
      if (current > own) {
        fence(own);
        return CompletableFuture.completedFuture(own);
      }
      return takeNewEpoch();
    });
  }

  private CompletionStage<Long> takeNewEpoch() {
    return redis.registry().incr(RegistryKeys.epoch(nodeId)).thenCompose(e -> lease(e).thenCompose(r -> {
      if (r == -1L) {
        fence(e);
        return CompletableFuture.completedFuture(e);
      }
      acquired(e);
      log.warn("node {} re-took its lease with new epoch {}", nodeId, e);
      return redis.registry().zadd(RegistryKeys.NODES_LIVE, wallMillis.getAsLong(), nodeId).thenApply(x -> {
        reEpochListeners.forEach(l -> {
          try {
            l.accept(e);
          } catch (RuntimeException ex) {
            log.warn("re-epoch listener failed", ex);
          }
        });
        return e;
      });
    }));
  }

  private CompletionStage<Long> lease(long e) {
    return LEASE.run(redis.registry(), RegistryKeys.node(nodeId), Long.toString(e), Integer.toString(LEASE_TTL_SEC));
  }

  private void fence(long e) {
    if (fenced.compareAndSet(false, true)) {
      node.draining(true);
      node.leaseAlive(false);
      exit.exit(2, "node " + nodeId + " fenced: epoch " + e + " superseded by a newer incarnation");
    }
  }

  public void onReEpoch(LongConsumer listener) {
    reEpochListeners.add(listener);
  }

  public long epoch() {
    return epoch;
  }

  /** {@code SET node:draining:{id} 1 EX 900}, async; a failure is only logged (spec §7.5). */
  public void markDraining() {
    try {
      redis.registry().set(RegistryKeys.draining(nodeId), "1", io.lettuce.core.SetArgs.Builder.ex(900))
          .whenComplete((v, e) -> {
            if (e != null) {
              log.warn("node {} could not mark itself draining: {}", nodeId, e.toString());
            }
          });
    } catch (RuntimeException e) {
      log.warn("node {} could not mark itself draining: {}", nodeId, e.toString());
    }
  }

  @Override
  public void stop() {
    running = false;
    if (renewTask != null) {
      renewTask.cancel(false);
    }
    if (epoch == 0) {
      return;
    }
    try {
      RELEASE.run(redis.registry(), RegistryKeys.node(nodeId), Long.toString(epoch)).toCompletableFuture()
          .get(2, TimeUnit.SECONDS);
      redis.registry().del(RegistryKeys.draining(nodeId)).get(2, TimeUnit.SECONDS);
      // Only drop the live marker if no newer incarnation holds the lease.
      if (redis.registry().get(RegistryKeys.node(nodeId)).get(2, TimeUnit.SECONDS) == null) {
        redis.registry().zrem(RegistryKeys.NODES_LIVE, nodeId).get(2, TimeUnit.SECONDS);
      }
    } catch (Exception e) {
      log.warn("node {} could not release its lease: {}", nodeId, e.toString());
    }
    node.leaseAlive(false);
  }

  @Override
  public boolean isRunning() {
    return running;
  }

  @Override
  public int getPhase() {
    return PHASE;
  }
}
