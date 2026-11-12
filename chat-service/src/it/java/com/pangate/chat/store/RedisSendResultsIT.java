package com.pangate.chat.store;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.chat.config.ChatProperties;
import com.pangate.chat.core.ChatStores.StoredMessage;
import com.pangate.chat.core.SendResults;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

class RedisSendResultsIT {

  static final GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7.4.2"))
      .withExposedPorts(6379);

  @BeforeAll
  static void start() {
    redis.start();
  }

  @AfterAll
  static void stop() {
    redis.stop();
  }

  /** Final review minor: two requests with the same key on one pod both get the result, not only the last one. */
  @Test
  void twoWaitersOfOneKeyBothGetTheResult() throws Exception {
    ChatProperties.Redis cfg = new ChatProperties.Redis("redis://" + redis.getHost() + ":" + redis.getMappedPort(6379),
        Duration.ofSeconds(2), Duration.ofSeconds(60));
    try (RedisSendResults r = new RedisSendResults(cfg, "pod-a");
        ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
      StoredMessage m = new StoredMessage("c1", 7, "m7", "u1", "k1", "hi", "", List.of(), 1_000);
      String key = SendResults.key("c1", "u1", "k1");
      CompletableFuture<Optional<StoredMessage>> first = CompletableFuture.supplyAsync(
          () -> r.await(key, Duration.ofSeconds(5)), pool);
      CompletableFuture<Optional<StoredMessage>> second = CompletableFuture.supplyAsync(
          () -> r.await(key, Duration.ofSeconds(5)), pool);
      Thread.sleep(500);   // both are waiting
      r.publish(RedisSendResults.replyTo("pod-a"), m);
      assertThat(first.get()).hasValueSatisfying(s -> assertThat(s.convSeq()).isEqualTo(7));
      assertThat(second.get()).hasValueSatisfying(s -> assertThat(s.convSeq()).isEqualTo(7));
    }
  }
}
