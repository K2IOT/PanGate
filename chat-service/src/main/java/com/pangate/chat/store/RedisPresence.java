package com.pangate.chat.store;

import com.pangate.chat.config.ChatProperties;
import com.pangate.chat.core.PresenceState;
import com.pangate.chat.core.StoreUnavailableException;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;

/**
 * CH3 spec §6.1 in Redis: {@code pres:{user}} (set of {@code <node>/<conn>}), {@code presnode:{node}:{epoch}} (set of
 * {@code <user>/<conn>}), both TTL 24 h renewed on each write; {@code presgone:{user}}, {@code lastseen:{user}},
 * {@code presev:{eventId}} (1 h). Watches (§6.3): {@code watchers:{user}} and {@code watchconv:{conv}} (sorted sets,
 * score = expiry ms) and {@code watching:{viewer}/{device}} (the device's current list, for replacement). A Redis
 * failure is a {@link StoreUnavailableException}: presence is then unknown.
 */
public final class RedisPresence implements PresenceState, com.pangate.chat.core.Watchers, AutoCloseable {

  static final Duration SET_TTL = Duration.ofHours(24);
  static final Duration EVENT_TTL = Duration.ofHours(1);
  static final long RECONNECT_MS = 1_000;

  private final RedisClient client;
  private volatile StatefulRedisConnection<String, String> conn;
  private volatile long nextConnectAt;

  public RedisPresence(ChatProperties.Redis cfg) {
    RedisURI uri = RedisURI.create(cfg.uri());
    uri.setTimeout(cfg.timeout());
    this.client = RedisClient.create(uri);
  }

  private RedisCommands<String, String> redis() {
    StatefulRedisConnection<String, String> c = conn;
    if (c != null) {
      return c.sync();
    }
    synchronized (this) {
      if (conn == null) {
        if (System.currentTimeMillis() < nextConnectAt) {
          throw new StoreUnavailableException("redis: reconnect pending", null);
        }
        try {
          conn = client.connect();
        } catch (RuntimeException e) {
          nextConnectAt = System.currentTimeMillis() + RECONNECT_MS;
          throw new StoreUnavailableException("redis: " + e.getMessage(), e);
        }
      }
      return conn.sync();
    }
  }

  private interface Op<T> {
    T run(RedisCommands<String, String> r);
  }

  private <T> T call(Op<T> op) {
    try {
      return op.run(redis());
    } catch (StoreUnavailableException e) {
      throw e;
    } catch (RuntimeException e) {
      throw new StoreUnavailableException("redis: " + e.getMessage(), e);
    }
  }

  @Override
  public boolean add(String userId, String node, long epoch, String connId) {
    return call(r -> {
      long added = r.sadd("pres:" + userId, node + "/" + connId);
      r.expire("pres:" + userId, SET_TTL);
      r.sadd("presnode:" + node + ":" + epoch, userId + "/" + connId);
      r.expire("presnode:" + node + ":" + epoch, SET_TTL);
      return added == 1 && r.scard("pres:" + userId) == 1;
    });
  }

  @Override
  public boolean remove(String userId, String node, long epoch, String connId) {
    return call(r -> {
      long removed = r.srem("pres:" + userId, node + "/" + connId);
      r.srem("presnode:" + node + ":" + epoch, userId + "/" + connId);
      return removed == 1 && r.scard("pres:" + userId) == 0;
    });
  }

  @Override
  public List<String> dropNode(String node, long epoch) {
    return call(r -> {
      List<String> emptied = new ArrayList<>();
      String key = "presnode:" + node + ":" + epoch;
      for (String uc : r.smembers(key)) {
        int slash = uc.indexOf('/');
        String user = uc.substring(0, slash);
        if (r.srem("pres:" + user, node + "/" + uc.substring(slash + 1)) == 1 && r.scard("pres:" + user) == 0) {
          emptied.add(user);
        }
      }
      r.del(key);
      return emptied;
    });
  }

  @Override
  public boolean online(String userId) {
    return call(r -> r.scard("pres:" + userId) > 0);
  }

  @Override
  public boolean seen(String eventId) {
    return call(r -> r.set("presev:" + eventId, "1", SetArgs.Builder.nx().ex(EVENT_TTL)) == null);
  }

  @Override
  public void markGone(String userId, long atMs) {
    call(r -> r.setex("presgone:" + userId, SET_TTL.toSeconds(), Long.toString(atMs)));
  }

  @Override
  public OptionalLong gone(String userId) {
    return call(r -> parse(r.get("presgone:" + userId)));
  }

  @Override
  public void clearGone(String userId) {
    call(r -> r.del("presgone:" + userId));
  }

  @Override
  public OptionalLong lastSeen(String userId) {
    return call(r -> parse(r.get("lastseen:" + userId)));
  }

  @Override
  public void putLastSeen(String userId, long atMs) {
    call(r -> r.set("lastseen:" + userId, Long.toString(atMs)));
  }

  // --- watches (CH3 spec §6.3) -------------------------------------------------------------------------------

  @Override
  public void replace(String viewer, String device, List<com.pangate.contracts.v1.PresenceWatch.Entry> valid,
      long expiresAtMs) {
    call(r -> {
      String dev = viewer + "/" + device;
      String list = "watching:" + dev;
      for (String old : r.smembers(list)) {
        int bar = old.indexOf('|');
        String user = old.substring(0, bar);
        String conv = old.substring(bar + 1);
        r.zrem("watchers:" + user, dev + "/" + conv);
        r.zrem("watchconv:" + conv, dev);
      }
      r.del(list);
      for (com.pangate.contracts.v1.PresenceWatch.Entry e : valid) {
        String w = "watchers:" + e.getUserId();
        String c = "watchconv:" + e.getConversationId();
        r.zadd(w, expiresAtMs, dev + "/" + e.getConversationId());
        r.pexpireat(w, expiresAtMs);
        r.zadd(c, expiresAtMs, dev);
        r.pexpireat(c, expiresAtMs);
        r.sadd(list, e.getUserId() + "|" + e.getConversationId());
      }
      if (!valid.isEmpty()) {
        r.pexpireat(list, expiresAtMs);
      }
      return null;
    });
  }

  @Override
  public List<String> watchersOf(String userId, long nowMs) {
    return call(r -> {
      String key = "watchers:" + userId;
      r.zremrangebyscore(key, io.lettuce.core.Range.create(Double.NEGATIVE_INFINITY, (double) nowMs));
      return r.zrangebyscore(key, io.lettuce.core.Range.create((double) nowMs, Double.POSITIVE_INFINITY));
    });
  }

  @Override
  public java.util.Set<String> devicesWithOpen(String convId) {
    long now = System.currentTimeMillis();
    return call(r -> new java.util.HashSet<>(r.zrangebyscore("watchconv:" + convId,
        io.lettuce.core.Range.create((double) now, Double.POSITIVE_INFINITY))));
  }

  private static OptionalLong parse(String v) {
    return v == null ? OptionalLong.empty() : OptionalLong.of(Long.parseLong(v));
  }

  @Override
  public void close() {
    if (conn != null) {
      conn.close();
    }
    client.shutdown(Duration.ZERO, Duration.ofSeconds(1));
  }
}
