package com.pangate.gateway.registry;

import com.pangate.gateway.config.GatewayProperties;
import com.pangate.gateway.config.RedisTopology;
import com.pangate.gateway.it.InfraContainers;
import com.pangate.gateway.lifecycle.RedisConnections;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.sync.RedisCommands;
import java.time.Duration;

/** A connected RedisConnections plus a plain sync client on the shared Redis container; pause/unpause helpers. */
final class RedisFixture implements AutoCloseable {

  final RedisConnections redis;
  final RedisClient client;
  final RedisCommands<String, String> sync;

  RedisFixture() {
    this(Duration.ofMillis(500));
  }

  RedisFixture(Duration commandTimeout) {
    InfraContainers.flushRedis();
    String uri = InfraContainers.redisUri();
    redis = new RedisConnections(new GatewayProperties.Redis(RedisTopology.STANDALONE, uri, uri, commandTimeout));
    redis.connect();
    client = RedisClient.create(uri);
    sync = client.connect().sync();
  }

  static void pause() {
    var c = InfraContainers.redis();
    c.getDockerClient().pauseContainerCmd(c.getContainerId()).exec();
  }

  static void unpause() {
    var c = InfraContainers.redis();
    c.getDockerClient().unpauseContainerCmd(c.getContainerId()).exec();
  }

  @Override
  public void close() {
    redis.close();
    client.shutdown();
  }
}
