package com.pangate.account.infrastructure.redis;

import com.pangate.account.application.port.AttemptCounter;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SocketOptions;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import java.time.Duration;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Counters shared by every pod (account spec §7): {@code acct:n:<key>} (INCR, expiry set by the first increment) and
 * {@code acct:b:<key>} (a block, its TTL the time left). Any Redis failure answers from {@link InMemoryAttemptCounter}
 * so the limits still hold per pod; a failed connect is retried at most once a second.
 */
public final class RedisAttemptCounter implements AttemptCounter, AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(RedisAttemptCounter.class);
  static final long RECONNECT_MS = 1_000;

  private final RedisClient client;
  private final InMemoryAttemptCounter fallback;
  private final java.util.concurrent.locks.ReentrantLock connectLock = new java.util.concurrent.locks.ReentrantLock();
  /** INCR and the window's expiry in one step; also restores an expiry a failed earlier call left missing. */
  static final String INCREMENT = "local n = redis.call('INCR', KEYS[1]) "
      + "if n == 1 or redis.call('PTTL', KEYS[1]) == -1 then redis.call('PEXPIRE', KEYS[1], ARGV[1]) end return n";
  /** DECR only a key whose window is still running, never below zero. */
  static final String DECREMENT = "if redis.call('GET', KEYS[1]) and tonumber(redis.call('GET', KEYS[1])) > 0 "
      + "then return redis.call('DECR', KEYS[1]) end return 0";
  private volatile StatefulRedisConnection<String, String> conn;
  private volatile long nextConnectAt;

  public RedisAttemptCounter(String uri, Duration timeout, InMemoryAttemptCounter fallback) {
    RedisURI u = RedisURI.create(uri);
    u.setTimeout(timeout);
    this.client = RedisClient.create(u);
    client.setOptions(ClientOptions.builder()
        .socketOptions(SocketOptions.builder().connectTimeout(timeout).build())
        .disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS).build());
    this.fallback = fallback;
  }

  private RedisCommands<String, String> redis() {
    StatefulRedisConnection<String, String> c = conn;
    if (c != null && c.isOpen()) {
      return c.sync();
    }
    if (!connectLock.tryLock()) {
      return null;   // another thread is connecting: answer from the fallback rather than wait
    }
    try {
      if ((conn == null || !conn.isOpen()) && System.currentTimeMillis() >= nextConnectAt) {
        try {
          conn = client.connect();
        } catch (RuntimeException e) {
          nextConnectAt = System.currentTimeMillis() + RECONNECT_MS;
          log.debug("redis not connected: {}", e.toString());
        }
      }
      return conn == null || !conn.isOpen() ? null : conn.sync();
    } finally {
      connectLock.unlock();
    }
  }

  @Override
  public long increment(String key, Duration window) {
    try {
      RedisCommands<String, String> r = redis();
      if (r != null) {
        Long n = r.eval(INCREMENT, io.lettuce.core.ScriptOutputType.INTEGER, new String[] {"acct:n:" + key},
            Long.toString(window.toMillis()));
        return n;
      }
    } catch (RuntimeException e) {
      log.debug("redis increment failed: {}", e.toString());
    }
    return fallback.increment(key, window);
  }

  @Override
  public long count(String key) {
    try {
      RedisCommands<String, String> r = redis();
      if (r != null) {
        String v = r.get("acct:n:" + key);
        return v == null ? 0 : Long.parseLong(v);
      }
    } catch (RuntimeException e) {
      log.debug("redis get failed: {}", e.toString());
    }
    return fallback.count(key);
  }

  @Override
  public void decrement(String key) {
    fallback.decrement(key);
    try {
      RedisCommands<String, String> r = redis();
      if (r != null) {
        r.eval(DECREMENT, io.lettuce.core.ScriptOutputType.INTEGER, new String[] {"acct:n:" + key});
      }
    } catch (RuntimeException e) {
      log.debug("redis decrement failed: {}", e.toString());
    }
  }

  @Override
  public void clear(String key) {
    fallback.clear(key);
    try {
      RedisCommands<String, String> r = redis();
      if (r != null) {
        r.del("acct:n:" + key);
      }
    } catch (RuntimeException e) {
      log.debug("redis del failed: {}", e.toString());
    }
  }

  @Override
  public void block(String key, Duration duration) {
    fallback.block(key, duration);
    try {
      RedisCommands<String, String> r = redis();
      if (r != null) {
        r.psetex("acct:b:" + key, duration.toMillis(), "1");
      }
    } catch (RuntimeException e) {
      log.debug("redis block failed: {}", e.toString());
    }
  }

  @Override
  public Optional<Duration> blockedFor(String key) {
    try {
      RedisCommands<String, String> r = redis();
      if (r != null) {
        long ms = r.pttl("acct:b:" + key);
        return ms > 0 ? Optional.of(Duration.ofMillis(ms)) : fallback.blockedFor(key);
      }
    } catch (RuntimeException e) {
      log.debug("redis pttl failed: {}", e.toString());
    }
    return fallback.blockedFor(key);
  }

  @Override
  public void close() {
    if (conn != null) {
      conn.close();
    }
    client.shutdown(Duration.ZERO, Duration.ofSeconds(1));
  }
}
