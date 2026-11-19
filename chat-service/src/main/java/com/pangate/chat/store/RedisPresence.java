package com.pangate.chat.store;

import com.pangate.chat.config.ChatProperties;
import com.pangate.chat.core.PresenceState;
import com.pangate.chat.core.StoreUnavailableException;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
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
      // final review I7 + Codex review of PR #6: checked after the writes. dropNode writes its tombstone before its
      // scan, so either the scan sees these entries or this check sees the tombstone and takes them back.
      if (r.exists("presdead:" + node + ":" + epoch) > 0) {
        r.srem("pres:" + userId, node + "/" + connId);
        r.srem("presnode:" + node + ":" + epoch, userId + "/" + connId);
        return false;
      }
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
      r.setex("presdead:" + node + ":" + epoch, SET_TTL.toSeconds(), "1");   // first: see add()
      for (String uc : r.smembers(key)) {
        int slash = uc.indexOf('/');
        String user = uc.substring(0, slash);
        // batch 1 review I2: a retry finds the entry gone already; the user still counts as emptied
        r.srem("pres:" + user, node + "/" + uc.substring(slash + 1));
        if (r.scard("pres:" + user) == 0) {
          emptied.add(user);
        }
      }
      return emptied;
    });
  }

  @Override
  public void forgetNode(String node, long epoch) {
    call(r -> r.del("presnode:" + node + ":" + epoch));
  }

  @Override
  public boolean online(String userId) {
    return call(r -> r.scard("pres:" + userId) > 0);
  }

  /**
   * CH4 deferred minor: one SCARD per follower, all written before any answer is awaited (Lettuce pipelines
   * commands on one connection), so a page of 1 000 costs about one round trip instead of 1 000.
   */
  @Override
  public java.util.Set<String> onlineOf(java.util.Collection<String> userIds) {
    return call(r -> {
      var async = conn.async();
      List<String> ids = List.copyOf(userIds);
      List<io.lettuce.core.RedisFuture<Long>> counts = new java.util.ArrayList<>(ids.size());
      for (String u : ids) {
        counts.add(async.scard("pres:" + u));
      }
      java.util.Set<String> out = new java.util.HashSet<>();
      for (int i = 0; i < ids.size(); i++) {
        try {
          if (counts.get(i).get(5, java.util.concurrent.TimeUnit.SECONDS) > 0) {
            out.add(ids.get(i));
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new StoreUnavailableException("redis: interrupted", e);
        } catch (java.util.concurrent.ExecutionException | java.util.concurrent.TimeoutException e) {
          throw new StoreUnavailableException("redis: " + e, e);
        }
      }
      return out;
    });
  }

  @Override
  public boolean handled(String eventId) {
    return call(r -> r.exists("presev:" + eventId) > 0);
  }

  @Override
  public void markHandled(String eventId) {
    call(r -> r.setex("presev:" + eventId, EVENT_TTL.toSeconds(), "1"));
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

  // --- offline graces (final review I6): one sorted set, score = due time ---

  @Override
  public void scheduleGrace(String userId, long dueAtMs) {
    call(r -> r.zadd("presgrace", dueAtMs, userId));
  }

  @Override
  public List<String> dueGraces(long nowMs) {
    return call(r -> r.zrangebyscore("presgrace", io.lettuce.core.Range.create(Double.NEGATIVE_INFINITY,
        (double) nowMs), io.lettuce.core.Limit.create(0, 1000)));
  }

  @Override
  public boolean claimGrace(String userId) {
    return call(r -> r.zrem("presgrace", userId) == 1);
  }

  // --- review 2026-11-19 (F6): open channels, chanopen:<user> = conv ids, kept a day after the last open ---------

  @Override
  public void channelOpened(String userId, String convId) {
    call(r -> {
      r.sadd("chanopen:" + userId, convId);
      return r.expire("chanopen:" + userId, SET_TTL);
    });
  }

  @Override
  public void channelClosed(String userId, String convId) {
    call(r -> r.srem("chanopen:" + userId, convId));
  }

  @Override
  public void clearOpenChannels(String userId) {
    call(r -> r.del("chanopen:" + userId));
  }

  /** One SISMEMBER per user, all written before any answer is awaited (pipelined like {@link #onlineOf}). */
  @Override
  public java.util.Set<String> haveOpen(String convId, java.util.Collection<String> userIds) {
    if (userIds.isEmpty()) {
      return java.util.Set.of();
    }
    return call(r -> {
      var async = conn.async();
      List<String> ids = List.copyOf(userIds);
      List<io.lettuce.core.RedisFuture<Boolean>> answers = new java.util.ArrayList<>(ids.size());
      for (String u : ids) {
        answers.add(async.sismember("chanopen:" + u, convId));
      }
      java.util.Set<String> out = new java.util.HashSet<>();
      for (int i = 0; i < ids.size(); i++) {
        try {
          if (Boolean.TRUE.equals(answers.get(i).get(5, java.util.concurrent.TimeUnit.SECONDS))) {
            out.add(ids.get(i));
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new StoreUnavailableException("redis: interrupted", e);
        } catch (java.util.concurrent.ExecutionException | java.util.concurrent.TimeoutException e) {
          throw new StoreUnavailableException("redis: " + e, e);
        }
      }
      return out;
    });
  }

  @Override
  public void cancelGrace(String userId) {
    call(r -> r.zrem("presgrace", userId));
  }

  // --- watches (CH3 spec §6.3) -------------------------------------------------------------------------------

  @Override
  public void replace(String viewer, String device, List<com.pangate.contracts.v1.PresenceWatch.Entry> valid,
      java.util.Set<String> open, long expiresAtMs) {
    call(r -> {
      String dev = viewer + "/" + device;
      String list = "watching:" + dev;
      r.expire("pres:" + viewer, SET_TTL);   // final review I8: a watching device is connected; keep its set alive
      for (String old : r.smembers(list)) {
        int bar = old.indexOf('|');
        String user = old.substring(0, bar);
        String conv = old.substring(bar + 1);
        if (!user.isEmpty()) {
          r.zrem("watchers:" + user, dev + "/" + conv);
        }
        r.zrem("watchconv:" + conv, dev);
      }
      r.del(list);
      for (com.pangate.contracts.v1.PresenceWatch.Entry e : valid) {
        String w = "watchers:" + e.getUserId();
        r.zadd(w, expiresAtMs, dev + "/" + e.getConversationId());
        r.pexpireat(w, expiresAtMs);
        r.sadd(list, e.getUserId() + "|" + e.getConversationId());
      }
      for (String conv : open) {   // "|<conv>": open only, no presence entry
        String c = "watchconv:" + conv;
        r.zadd(c, expiresAtMs, dev);
        r.pexpireat(c, expiresAtMs);
        r.sadd(list, "|" + conv);
      }
      if (!valid.isEmpty() || !open.isEmpty()) {
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
