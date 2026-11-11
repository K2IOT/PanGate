package com.pangate.router.nodes;

import static org.assertj.core.api.Assertions.assertThat;

import io.lettuce.core.RedisClient;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class RedisNodeSourceIT {

  @Container
  static GenericContainer<?> redis = new GenericContainer<>("redis:7.4.2").withExposedPorts(6379);

  @Test
  void readsLeaseAddressAndDraining() throws Exception {
    RedisClient client = RedisClient.create("redis://" + redis.getHost() + ":" + redis.getMappedPort(6379));
    try (var c = client.connect()) {
      var s = c.sync();
      long now = 1_000_000;
      s.zadd("nodes:live", now - 1_000, "a");
      s.zadd("nodes:live", now - 2_000, "b");
      s.zadd("nodes:live", now - 2_000, "noaddr");
      s.zadd("nodes:live", now - 60_000, "old");
      s.set("node:{a}", "3");
      s.set("node:addr:{a}", "10.0.0.1:9090");
      s.set("node:{b}", "7");
      s.set("node:addr:{b}", "10.0.0.2:9090");
      s.set("node:draining:{b}", "1");
      s.set("node:{noaddr}", "1");
      s.set("node:{old}", "1");
      s.set("node:addr:{old}", "10.0.0.9:9090");

      var nodes = new RedisNodeSource(c.async(), Duration.ofSeconds(2)).fetch(now);

      assertThat(nodes).containsOnlyKeys("a", "b");
      assertThat(nodes.get("a")).isEqualTo(new Node("a", 3, "10.0.0.1:9090", false));
      assertThat(nodes.get("b").draining()).isTrue();
    } finally {
      client.shutdown();
    }
  }
}
