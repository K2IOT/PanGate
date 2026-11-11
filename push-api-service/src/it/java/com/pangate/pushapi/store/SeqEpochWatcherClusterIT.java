package com.pangate.pushapi.store;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.pangate.contracts.testkit.SeqKeys;
import com.pangate.pushapi.config.PushApiProperties;
import com.pangate.pushapi.core.PushApiKeys;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * Failover detection on a Redis Cluster (push-api spec §3.2): three masters on the host network, no persistence. A
 * restarted master loses its data and gets a new run_id; the watcher bumps the epochs of that master's slots only.
 */
class SeqEpochWatcherClusterIT {

  static final List<GenericContainer<?>> masters = new ArrayList<>();
  static final List<Integer> ports = new ArrayList<>();
  static RedisState state;

  @BeforeAll
  static void boot() throws Exception {
    for (int i = 0; i < 3; i++) {
      int port;
      try (ServerSocket s = new ServerSocket(0)) {
        port = s.getLocalPort();
      }
      ports.add(port);
      GenericContainer<?> c = new GenericContainer<>(DockerImageName.parse("redis:7.4.2")).withNetworkMode("host")
          .withCommand("redis-server", "--port", Integer.toString(port), "--cluster-enabled", "yes",
              "--cluster-config-file", "nodes-" + port + ".conf", "--save", "", "--appendonly", "no")
          .waitingFor(Wait.forLogMessage(".*Ready to accept connections.*", 1));
      c.start();
      masters.add(c);
    }
    var created = masters.get(0).execInContainer("redis-cli", "--cluster", "create", "127.0.0.1:" + ports.get(0),
        "127.0.0.1:" + ports.get(1), "127.0.0.1:" + ports.get(2), "--cluster-yes");
    assertThat(created.getExitCode()).as(created.getStdout() + created.getStderr()).isZero();
    await().atMost(Duration.ofSeconds(30)).until(() -> masters.stream().allMatch(SeqEpochWatcherClusterIT::clusterOk));
    state = RedisState.connect(new PushApiProperties.Redis("redis://127.0.0.1:" + ports.get(0), "cluster",
        Duration.ofSeconds(1)));
  }

  static boolean clusterOk(GenericContainer<?> c) {
    try {
      int port = ports.get(masters.indexOf(c));
      return c.execInContainer("redis-cli", "-p", Integer.toString(port), "cluster", "info").getStdout()
          .contains("cluster_state:ok");
    } catch (Exception e) {
      return false;
    }
  }

  @AfterAll
  static void shutdown() {
    if (state != null) {
      state.close();
    }
    masters.forEach(GenericContainer::stop);
  }

  @Test
  void aRestartedMasterBumpsOnlyItsSlots() throws Exception {
    List<RedisState.Shard> shards = state.shards();
    assertThat(shards).hasSize(3);
    int epochsSlot = SeqKeys.slot(SeqKeys.SEQ_EPOCHS);
    RedisState.Shard victim = shards.stream().filter(s -> epochsSlot < s.start() || epochsSlot > s.end())
        .findFirst().orElseThrow();
    RedisState.Shard other = shards.stream().filter(s -> s != victim).findFirst().orElseThrow();
    // epochs handed out before: 3 in the victim's range, 5 in another range
    state.async().hset(SeqKeys.SEQ_EPOCHS, Map.of(Integer.toString(victim.start()), "3",
        Integer.toString(other.start()), "5")).get();
    RedisSeqEpochs epochs = new RedisSeqEpochs(state);
    epochs.refreshNow();
    List<String> bumps = new CopyOnWriteArrayList<>();
    SeqEpochWatcher w = new SeqEpochWatcher(state, epochs, bumps::add);
    w.checkNow();
    assertThat(bumps).isEmpty();
    assertThat(state.async().hgetall(PushApiKeys.RUNIDS).get()).hasSize(3);

    GenericContainer<?> node = masterServing(victim);
    node.getDockerClient().restartContainerCmd(node.getContainerId()).exec();   // data lost, new run_id
    await().atMost(Duration.ofSeconds(60)).ignoreExceptions().until(() -> {
      w.checkNow();
      return !bumps.isEmpty();
    });

    assertThat(bumps).containsExactly("failover");
    Map<String, String> e = state.async().hgetall(SeqKeys.SEQ_EPOCHS).get();
    assertThat(e.get(Integer.toString(victim.start()))).isEqualTo("4");   // above the 3 handed out
    assertThat(e.get(Integer.toString(victim.end()))).isEqualTo("4");     // floor of the range + 1
    assertThat(e.get(Integer.toString(other.start()))).isEqualTo("5");    // other shard untouched
    assertThat(e).hasSize(victim.end() - victim.start() + 2);
    w.checkNow();
    assertThat(bumps).hasSize(1);   // once
  }

  static GenericContainer<?> masterServing(RedisState.Shard shard) throws Exception {
    for (int i = 0; i < masters.size(); i++) {
      String nodes = masters.get(i).execInContainer("redis-cli", "-p", Integer.toString(ports.get(i)), "cluster",
          "nodes").getStdout();
      for (String line : nodes.split("\n")) {
        if (line.contains("myself") && line.trim().endsWith(shard.start() + "-" + shard.end())) {
          return masters.get(i);
        }
      }
    }
    throw new IllegalStateException("no master serves " + shard.range());
  }
}
