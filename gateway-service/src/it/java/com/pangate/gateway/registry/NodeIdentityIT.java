package com.pangate.gateway.registry;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.gateway.lifecycle.ProcessExit;
import com.pangate.gateway.metrics.GatewayMetrics;
import com.pangate.gateway.server.NodeState;
import com.pangate.gateway.server.TickScheduler;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class NodeIdentityIT {

  private RedisFixture fx;
  private final TickScheduler tick = new TickScheduler();
  private final AtomicLong mono = new AtomicLong(1_000_000);
  private final List<String> exits = new CopyOnWriteArrayList<>();
  private final List<NodeIdentity> started = new ArrayList<>();

  @BeforeEach
  void setUp() {
    fx = new RedisFixture();
  }

  @AfterEach
  void tearDown() {
    started.forEach(NodeIdentity::stop);
    tick.shutdown();
    fx.close();
  }

  private NodeIdentity identity(NodeState node) {
    ProcessExit exit = (status, reason) -> exits.add(status + ":" + reason);
    NodeIdentity id = new NodeIdentity("n1", fx.redis, node, tick, mono::get, System::currentTimeMillis, exit,
        new GatewayMetrics(new SimpleMeterRegistry()));
    started.add(id);
    return id;
  }

  @Test
  void startTakesNewEpochAndLeaseWithTtl15() {
    NodeState node = new NodeState();
    NodeIdentity a = identity(node);

    a.start();

    assertThat(a.epoch()).isEqualTo(1);
    assertThat(node.epoch()).isEqualTo(1);
    assertThat(node.nodeId()).isEqualTo("n1");
    assertThat(node.leaseAlive()).isTrue();
    assertThat(fx.sync.get("node:{n1}")).isEqualTo("1");
    assertThat(fx.sync.ttl("node:{n1}")).isBetween(1L, 15L);
    assertThat(fx.sync.zscore("nodes:live", "n1")).isNotNull();
    assertThat(a.getPhase()).isEqualTo(Integer.MAX_VALUE - 1800);
  }

  @Test
  void restartWithLiveLeaseTakesOverWithHigherEpoch() {
    NodeIdentity a = identity(new NodeState());
    a.start();
    NodeIdentity b = identity(new NodeState());

    b.start();

    assertThat(b.epoch()).isEqualTo(2);
    assertThat(fx.sync.get("node:{n1}")).isEqualTo("2");
    assertThat(exits).isEmpty();
  }

  @Test
  void olderIncarnationFencesOnRenew() throws Exception {
    NodeState nodeA = new NodeState();
    NodeIdentity a = identity(nodeA);
    a.start();
    identity(new NodeState()).start();

    a.renew().toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertThat(exits).singleElement().asString().startsWith("2:").contains("fenced");
    assertThat(nodeA.draining()).isTrue();
    assertThat(fx.sync.get("node:{n1}")).isEqualTo("2");
  }

  @Test
  void renewExtendsLease() throws Exception {
    NodeIdentity a = identity(new NodeState());
    a.start();
    fx.sync.expire("node:{n1}", 3);

    a.renew().toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertThat(fx.sync.ttl("node:{n1}")).isGreaterThan(10L);
  }

  @Test
  void leaseLostAfter15sThenReEpochsOnRecovery() throws Exception {
    NodeState node = new NodeState();
    NodeIdentity a = identity(node);
    a.start();
    List<Long> reEpochs = new CopyOnWriteArrayList<>();
    a.onReEpoch(reEpochs::add);
    RedisFixture.pause();
    try {
      a.renew().toCompletableFuture().get(5, TimeUnit.SECONDS);
      assertThat(node.leaseAlive()).isTrue();
      mono.addAndGet(16_000);
      a.renew().toCompletableFuture().get(5, TimeUnit.SECONDS);
      assertThat(node.leaseAlive()).isFalse();
    } finally {
      RedisFixture.unpause();
    }

    a.renew().toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertThat(node.leaseAlive()).isTrue();
    // An INCR sent while Redis was paused times out client-side but still runs when it resumes: the new epoch is
    // larger than 1, not necessarily 2.
    long e = a.epoch();
    assertThat(e).isGreaterThan(1);
    assertThat(node.epoch()).isEqualTo(e);
    assertThat(reEpochs).containsExactly(e);
    assertThat(fx.sync.get("node:{n1}")).isEqualTo(Long.toString(e));
    assertThat(exits).isEmpty();
  }

  @Test
  void stopDeletesOnlyOwnLease() {
    NodeIdentity a = identity(new NodeState());
    a.start();
    NodeIdentity b = identity(new NodeState());
    b.start();

    a.stop();

    assertThat(fx.sync.get("node:{n1}")).isEqualTo("2");
    b.stop();
    assertThat(fx.sync.get("node:{n1}")).isNull();
    assertThat(fx.sync.zscore("nodes:live", "n1")).isNull();
  }
  @Test
  void staleProcessThatLostItsLeaseFencesInsteadOfStealingFromNewerIncarnation() throws Exception {
    NodeState nodeA = new NodeState();
    NodeIdentity a = identity(nodeA);
    a.start();
    NodeState nodeB = new NodeState();
    NodeIdentity b = identity(nodeB);
    b.start();
    mono.addAndGet(16_000);  // A was stalled (GC/VM pause) for longer than the lease TTL

    a.renew().toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertThat(exits).singleElement().asString().startsWith("2:").contains("fenced");
    assertThat(nodeA.draining()).isTrue();
    assertThat(fx.sync.get("node:{n1}")).isEqualTo("2");
    assertThat(fx.sync.get("node:epoch:{n1}")).isEqualTo("2");
  }

  @Test
  void markDrainingSetsKeyForFifteenMinutesAndStopDeletesIt() {
    NodeIdentity id = identity(new NodeState());
    id.start();
    id.markDraining();
    org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(5))
        .until(() -> "1".equals(fx.sync.get("node:draining:{n1}")));
    assertThat(fx.sync.ttl("node:draining:{n1}")).isBetween(890L, 900L);
    id.stop();
    assertThat(fx.sync.exists("node:draining:{n1}")).isZero();
  }
}
