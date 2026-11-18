package com.pangate.notify.infrastructure.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

/** One Redis connection, opened on first use: Redis down at start-up must not stop the service. */
public final class LazyRedis implements AutoCloseable {

  private final RedisClient client;
  private StatefulRedisConnection<String, String> connection;   // guarded by this

  public LazyRedis(RedisClient client) {
    this.client = client;
  }

  public synchronized RedisCommands<String, String> sync() {
    if (connection == null || !connection.isOpen()) {
      connection = client.connect();
    }
    return connection.sync();
  }

  @Override
  public synchronized void close() {
    if (connection != null) {
      connection.close();
    }
  }
}
