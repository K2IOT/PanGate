package com.pangate.gateway.registry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.pangate.gateway.metrics.GatewayMetrics;
import com.pangate.gateway.server.NodeState;
import com.pangate.gateway.session.Session;
import com.pangate.gateway.test.LoopFixture;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.net.InetAddress;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RegistryWriterIT {

  private RedisFixture fx;
  private final ScheduledExecutorService sched = Executors.newSingleThreadScheduledExecutor();
  private RetryQueue retry;
  private RegistryWriter writer;

  @BeforeEach
  void setUp() {
    fx = new RedisFixture();
    NodeState node = new NodeState();
    node.nodeId("n1");
    node.epoch(7);
    GatewayMetrics metrics = new GatewayMetrics(new SimpleMeterRegistry());
    retry = new RetryQueue((r, d) -> sched.schedule(r, d, TimeUnit.MILLISECONDS), 10_000, metrics);
    writer = new RegistryWriter(fx.redis, node, retry, metrics);
  }

  @AfterEach
  void tearDown() {
    sched.shutdownNow();
    fx.close();
  }

  private static Session session(String principal, String connId, long connectedAtMs) {
    LoopFixture f = new LoopFixture();
    Session s = f.tracker.admit(f.channel, f.loop, InetAddress.getLoopbackAddress());
    s.activate(new Session.Identity("t1", principal, "d1", "android", "1.0", Set.of()), connId, connectedAtMs,
        f.wallNow(), f.wallNow() + 3_600, 0, List.of());
    return s;
  }

  private static <T> T get(java.util.concurrent.CompletionStage<T> st) throws Exception {
    return st.toCompletableFuture().get(5, TimeUnit.SECONDS);
  }

  @Test
  void registerWritesValueWithNodeEpochAndPlatform() throws Exception {
    Session s = session("t1:u1", "C1", 1234);
    get(writer.register(s, false));

    assertThat(fx.sync.hget("reg:{t1:u1}", "C1")).isEqualTo("n1|7|1234|android");
    // The finished write is not kept for the life of the session (idle heap budget).
    await().atMost(Duration.ofSeconds(2)).until(() -> {
      ((io.netty.channel.embedded.EmbeddedChannel) s.channel()).runPendingTasks();
      return s.registryWrite() == null;
    });
  }

  @Test
  void unregisterRemovesOnlyOwnConnId() throws Exception {
    get(writer.register(session("t1:u1", "C1", 1), false));
    get(writer.register(session("t1:u1", "C2", 2), false));

    writer.unregister("t1:u1", "C1");

    await().atMost(Duration.ofSeconds(5)).until(() -> !fx.sync.hexists("reg:{t1:u1}", "C1"));
    assertThat(fx.sync.hget("reg:{t1:u1}", "C2")).isEqualTo("n1|7|2|android");
  }

  @Test
  void singleModeReturnsAndReplacesPrevious() throws Exception {
    get(writer.register(session("t1:u2", "C1", 1), false));

    Map<String, String> prev = get(writer.register(session("t1:u2", "C2", 2), true));

    assertThat(prev).containsExactly(Map.entry("C1", "n1|7|1|android"));
    assertThat(fx.sync.hgetall("reg:{t1:u2}")).containsOnlyKeys("C2");
  }

  @Test
  void noscriptFallsBackToEval() throws Exception {
    get(writer.register(session("t1:u3", "C1", 1), false));
    fx.sync.scriptFlush();

    get(writer.register(session("t1:u3", "C2", 2), false));

    assertThat(fx.sync.hkeys("reg:{t1:u3}")).containsExactlyInAnyOrder("C1", "C2");
  }

  @Test
  void redisDownGoesToRetryQueue() throws Exception {
    Session s = session("t1:u4", "C1", 1);
    io.netty.channel.embedded.EmbeddedChannel loop = (io.netty.channel.embedded.EmbeddedChannel) s.channel();
    RedisFixture.pause();
    try {
      writer.activated(s);
      await().atMost(Duration.ofSeconds(5)).until(() -> retry.size() == 1);
    } finally {
      RedisFixture.unpause();
    }

    await().atMost(Duration.ofSeconds(10)).until(() -> {
      loop.runPendingTasks();  // retries run on the session's loop
      return fx.sync.hexists("reg:{t1:u4}", "C1");
    });
  }
  @Test
  void closeDuringInFlightRegisterLeavesNoEntry() throws Exception {
    // After a Redis restart the script cache is empty: EVALSHA answers NOSCRIPT and the EVAL fallback goes out
    // after the close's HDEL. Pausing Redis while both are sent makes that order deterministic.
    RedisFixture slow = new RedisFixture(Duration.ofSeconds(10));
    try {
      NodeState node = new NodeState();
      node.nodeId("n1");
      node.epoch(7);
      GatewayMetrics metrics = new GatewayMetrics(new SimpleMeterRegistry());
      RegistryWriter w = new RegistryWriter(slow.redis, node,
          new RetryQueue((r, d) -> sched.schedule(r, d, TimeUnit.MILLISECONDS), 10, metrics), metrics);
      slow.sync.scriptFlush();
      Session s = session("t1:u5", "C1", 1);

      RedisFixture.pause();
      try {
        w.activated(s);
        s.close(com.pangate.gateway.protocol.CloseCode.NORMAL, "bye");
        w.closed(s);
        Thread.sleep(300);
      } finally {
        RedisFixture.unpause();
      }

      Thread.sleep(1_500);
      assertThat(slow.sync.hexists("reg:{t1:u5}", "C1")).isFalse();
    } finally {
      slow.close();
    }
  }
}
