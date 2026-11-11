package com.pangate.pushapi.store;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import com.pangate.contracts.testkit.SeqKeys;
import com.pangate.pushapi.config.PushApiProperties;
import com.pangate.pushapi.core.PushApiKeys;
import com.pangate.pushapi.core.Seq;
import com.pangate.pushapi.core.SeqAllocator;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

/** The host port is fixed so the same URI reaches the container after a restart (a restart changes run_id). */
class SeqEpochWatcherIT {

  static int port;
  static GenericContainer<?> redis;
  static RedisState a;
  static RedisState b;

  @BeforeAll
  static void boot() throws Exception {
    try (ServerSocket s = new ServerSocket(0)) {
      port = s.getLocalPort();
    }
    // no RDB / AOF: a restart loses every key, like a failover to a replica that missed the last writes
    redis = new GenericContainer<>(DockerImageName.parse("redis:7.4.2")).withExposedPorts(6379)
        .withCommand("redis-server", "--save", "", "--appendonly", "no")
        .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig().withPortBindings(
            new PortBinding(Ports.Binding.bindPort(port), new ExposedPort(6379))));
    redis.start();
    a = RedisState.connect(props());
    b = RedisState.connect(props());
  }

  static PushApiProperties.Redis props() {
    return new PushApiProperties.Redis("redis://127.0.0.1:" + port, "standalone", Duration.ofSeconds(1));
  }

  @AfterAll
  static void shutdown() {
    a.close();
    b.close();
    redis.stop();
  }

  @BeforeEach
  void flush() throws Exception {
    a.async().flushall().get();
  }

  static Map<String, String> epochs() throws Exception {
    return a.async().hgetall(SeqKeys.SEQ_EPOCHS).get();
  }

  static void restartRedis() {
    redis.getDockerClient().restartContainerCmd(redis.getContainerId()).exec();
    await().atMost(Duration.ofSeconds(30)).ignoreExceptions()
        .until(() -> "PONG".equals(a.async().ping().get()) && "PONG".equals(b.async().ping().get()));
  }

  @Test
  void firstSightRecordsTheRunIdWithoutBumping() throws Exception {
    List<String> bumps = new CopyOnWriteArrayList<>();
    SeqEpochWatcher w = new SeqEpochWatcher(a, new RedisSeqEpochs(a), bumps::add);
    w.checkNow();
    assertThat(bumps).isEmpty();
    assertThat(epochs()).isEmpty();
    assertThat(a.async().hgetall(PushApiKeys.RUNIDS).get()).containsOnlyKeys("0-16383");
    w.checkNow();
    assertThat(bumps).isEmpty();
  }

  /** Review Focus 5. */
  @Test
  void restartBumpsEverySlotExactlyOnceWithTwoWatchers() throws Exception {
    a.async().hset(SeqKeys.SEQ_EPOCHS, Integer.toString(SeqKeys.slot("t1:u1")), "3").get();
    RedisSeqEpochs ea = new RedisSeqEpochs(a);
    RedisSeqEpochs eb = new RedisSeqEpochs(b);
    ea.refreshNow();
    eb.refreshNow();
    List<String> bumps = new CopyOnWriteArrayList<>();
    SeqEpochWatcher wa = new SeqEpochWatcher(a, ea, bumps::add);
    SeqEpochWatcher wb = new SeqEpochWatcher(b, eb, bumps::add);
    wa.checkNow();
    wb.checkNow();
    restartRedis();     // no persistence: seqepochs, runids and every seq are gone, run_id is new
    CountDownLatch go = new CountDownLatch(1);
    Thread ta = Thread.ofVirtual().start(() -> { waitFor(go); wa.checkNow(); });
    Thread tb = Thread.ofVirtual().start(() -> { waitFor(go); wb.checkNow(); });
    go.countDown();
    ta.join();
    tb.join();
    assertThat(bumps).containsExactly("failover");
    Map<String, String> e = epochs();
    assertThat(e).hasSize(SeqKeys.SLOTS);
    assertThat(e.get(Integer.toString(SeqKeys.slot("t1:u1")))).isEqualTo("4");   // above the 3 seen before
    assertThat(e.get("0")).isEqualTo("4");   // floor: the highest epoch this pod knew for the range, plus one
    assertThat(ea.epoch("t1:u1")).isEqualTo(4);
  }

  @Test
  void nextSeqCarriesTheNewEpoch() throws Exception {
    RedisSeqEpochs ep = new RedisSeqEpochs(a);
    SeqEpochWatcher w = new SeqEpochWatcher(a, ep, s -> { });
    SeqAllocator alloc = new SeqAllocator(new RedisSeqStore(a), ep, w::checkNow);
    w.checkNow();
    assertThat(alloc.forPrincipal("t1:u2")).isEqualTo(new Seq(1, 0));
    assertThat(alloc.forPrincipal("t1:u2")).isEqualTo(new Seq(2, 0));
    restartRedis();
    Seq after = alloc.forPrincipal("t1:u2");   // INCR gives 1 again: a regression, so the watcher runs first
    assertThat(after.epoch()).isEqualTo(1);
    assertThat(after.seq()).isEqualTo(2);
  }

  @Test
  void adminBumpOfSelectedSlots() throws Exception {
    RedisSeqEpochs ep = new RedisSeqEpochs(a);
    List<String> bumps = new CopyOnWriteArrayList<>();
    SeqEpochWatcher w = new SeqEpochWatcher(a, ep, bumps::add);
    assertThat(w.bump(Set.of(5, 7))).isEqualTo(2);
    assertThat(epochs()).containsOnly(Map.entry("5", "1"), Map.entry("7", "1"));
    assertThat(w.bumpAll()).isEqualTo(SeqKeys.SLOTS);
    assertThat(epochs().get("5")).isEqualTo("2");
    assertThat(bumps).containsExactly("admin", "admin");
  }

  static void waitFor(CountDownLatch l) {
    try {
      l.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /** Final review minor (spec §3.2): a range that no longer exists (resharding) loses its runids field. */
  @Test
  void rangesThatNoLongerExistAreDeleted() throws Exception {
    a.async().hset(PushApiKeys.RUNIDS, "0-100", "old").get();
    new SeqEpochWatcher(a, new RedisSeqEpochs(a), s -> { }).checkNow();
    assertThat(a.async().hgetall(PushApiKeys.RUNIDS).get()).containsOnlyKeys("0-16383");
  }

  /** Final review minor (spec §3.1): a bump started by a seq regression counts as reason=regression. */
  @Test
  void aBumpAfterARegressionCountsAsRegression() throws Exception {
    RedisSeqEpochs ep = new RedisSeqEpochs(a);
    List<String> bumps = new CopyOnWriteArrayList<>();
    SeqEpochWatcher w = new SeqEpochWatcher(a, ep, bumps::add);
    SeqAllocator alloc = new SeqAllocator(new RedisSeqStore(a), ep, () -> w.checkNow("regression"));
    w.checkNow();
    alloc.forPrincipal("t1:u3");
    restartRedis();
    alloc.forPrincipal("t1:u3");
    assertThat(bumps).containsExactly("regression");
  }

  /** Final review minor (spec §3.2): a cluster topology change triggers a check at once. */
  @Test
  void aTopologyChangeTriggersACheck() throws Exception {
    List<String> bumps = new CopyOnWriteArrayList<>();
    SeqEpochWatcher w = new SeqEpochWatcher(a, new RedisSeqEpochs(a), bumps::add);
    w.start(Duration.ofHours(1));
    try {
      await().atMost(Duration.ofSeconds(10)).until(() -> a.async().hexists(PushApiKeys.RUNIDS, "0-16383").get());
      a.async().hset(PushApiKeys.RUNIDS, "0-16383", "another-run").get();   // as if the shard's master changed
      a.resources().eventBus().publish(new io.lettuce.core.cluster.event.ClusterTopologyChangedEvent(List.of(),
          List.of()));
      await().atMost(Duration.ofSeconds(10)).until(() -> !bumps.isEmpty());
    } finally {
      w.close();
    }
  }
}
