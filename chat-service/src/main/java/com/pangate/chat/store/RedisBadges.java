package com.pangate.chat.store;

import com.pangate.chat.config.ChatProperties;
import com.pangate.chat.core.ChatStores;
import com.pangate.chat.core.StoreUnavailableException;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Perf 3a (spec §3.3): badge counters in {@code badge:{user}}, a hash with one field per DM / group
 * ({@code "<last>:<read>"}, each part the max ever written), {@code _total} and {@code _built_at} (ms). TTL 30 days,
 * renewed on every write. Each operation is one Lua script, so a raise and its total move together; a raise or drop on
 * a missing hash does nothing. A Redis failure is a {@link StoreUnavailableException}.
 */
public final class RedisBadges implements ChatStores.BadgeStore, AutoCloseable {

  static final Duration TTL = Duration.ofDays(30);
  static final long RECONNECT_MS = 1_000;
  static final long WAIT_SECONDS = 5;

  /** Shared Lua: parse a field, and raise one conversation (KEYS[1]) by the max of each part, adjusting the total. */
  private static final String LIB = """
      local function parse(v)
        if not v then return 0, 0 end
        local i = string.find(v, ':', 1, true)
        return tonumber(string.sub(v, 1, i - 1)), tonumber(string.sub(v, i + 1))
      end
      local function raise(key, conv, last, read)
        local l, r = parse(redis.call('HGET', key, conv))
        local nl = math.max(l, last)
        local nr = math.max(r, read)
        if nl ~= l or nr ~= r or l == 0 then
          redis.call('HSET', key, conv, string.format('%d:%d', nl, nr))
        end
        local d = math.max(0, nl - nr) - math.max(0, l - r)
        if d ~= 0 then
          redis.call('HINCRBY', key, '_total', string.format('%d', d))
        end
      end
      """;

  /** ARGV: conv, last, read, ttlMs. Nothing when the hash is missing. */
  static final String RAISE = LIB + """
      if redis.call('EXISTS', KEYS[1]) == 0 then return -1 end
      raise(KEYS[1], ARGV[1], tonumber(ARGV[2]), tonumber(ARGV[3]))
      redis.call('PEXPIRE', KEYS[1], ARGV[4])
      return 0
      """;

  /** ARGV: conv ('' = none), seq, builtAfterMs, ttlMs. -1 when missing or built before builtAfterMs. */
  static final String TOTAL = LIB + """
      local b = redis.call('HGET', KEYS[1], '_built_at')
      if not b or tonumber(b) < tonumber(ARGV[3]) then return -1 end
      if ARGV[1] ~= '' then
        raise(KEYS[1], ARGV[1], tonumber(ARGV[2]), 0)
        redis.call('PEXPIRE', KEYS[1], ARGV[4])
      end
      return tonumber(redis.call('HGET', KEYS[1], '_total') or '0')
      """;

  /** ARGV: conv, ttlMs. */
  static final String DROP = LIB + """
      local v = redis.call('HGET', KEYS[1], ARGV[1])
      if not v then return 0 end
      local l, r = parse(v)
      redis.call('HDEL', KEYS[1], ARGV[1])
      redis.call('HINCRBY', KEYS[1], '_total', string.format('%d', -math.max(0, l - r)))
      redis.call('PEXPIRE', KEYS[1], ARGV[2])
      return 0
      """;

  /**
   * ARGV: nowMs, ttlMs, then (conv, last, read) per inbox row. Each row is merged by max, fields of conversations no
   * longer in the inbox are removed, the total is counted again. ARGV is walked, never unpacked (10 000 rows).
   */
  static final String REBUILD = LIB + """
      local keep = {}
      for i = 3, #ARGV, 3 do
        keep[ARGV[i]] = true
        local l, r = parse(redis.call('HGET', KEYS[1], ARGV[i]))
        redis.call('HSET', KEYS[1], ARGV[i], string.format('%d:%d', math.max(l, tonumber(ARGV[i + 1])),
          math.max(r, tonumber(ARGV[i + 2]))))
      end
      local all = redis.call('HGETALL', KEYS[1])
      local total = 0
      for i = 1, #all, 2 do
        local f = all[i]
        if string.sub(f, 1, 1) ~= '_' then
          if keep[f] then
            local l, r = parse(all[i + 1])
            total = total + math.max(0, l - r)
          else
            redis.call('HDEL', KEYS[1], f)
          end
        end
      end
      redis.call('HSET', KEYS[1], '_total', string.format('%d', total), '_built_at', ARGV[1])
      redis.call('PEXPIRE', KEYS[1], ARGV[2])
      return total
      """;

  private final RedisClient client;
  private volatile StatefulRedisConnection<String, String> conn;
  private volatile long nextConnectAt;

  public RedisBadges(ChatProperties.Redis cfg) {
    RedisURI uri = RedisURI.create(cfg.uri());
    uri.setTimeout(cfg.timeout());
    this.client = RedisClient.create(uri);
  }

  /** Lazily connected; a failed connect is retried at most once a second. */
  private StatefulRedisConnection<String, String> connection() {
    StatefulRedisConnection<String, String> c = conn;
    if (c != null) {
      return c;
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
      return conn;
    }
  }

  static String key(String user) {
    return "badge:" + user;
  }

  private long eval(String script, String user, String... args) {
    try {
      Long v = connection().sync().eval(script, ScriptOutputType.INTEGER, new String[] {key(user)}, args);
      return v == null ? 0 : v;
    } catch (StoreUnavailableException e) {
      throw e;
    } catch (RuntimeException e) {
      throw new StoreUnavailableException("redis: " + e.getMessage(), e);
    }
  }

  private static String ttl() {
    return Long.toString(TTL.toMillis());
  }

  @Override
  public long totalWith(String user, String convId, long seq, long builtAfterMs) {
    return eval(TOTAL, user, convId == null ? "" : convId, Long.toString(seq), Long.toString(builtAfterMs), ttl());
  }

  @Override
  public void rebuild(String user, List<ChatStores.BadgeRow> snapshot, long nowMs) {
    List<String> args = new ArrayList<>(2 + snapshot.size() * 3);
    args.add(Long.toString(nowMs));
    args.add(ttl());
    for (ChatStores.BadgeRow r : snapshot) {
      args.add(r.convId());
      args.add(Long.toString(r.lastSeq()));
      args.add(Long.toString(r.readSeq()));
    }
    eval(REBUILD, user, args.toArray(String[]::new));
  }

  @Override
  public void raise(String user, String convId, long last, long read) {
    eval(RAISE, user, convId, Long.toString(last), Long.toString(read), ttl());
  }

  /** One script per user, all written before any answer is awaited (Lettuce pipelines on one connection). */
  @Override
  public void raiseAll(List<String> users, String convId, long last) {
    RedisAsyncCommands<String, String> async;
    List<RedisFuture<Long>> sent = new ArrayList<>(users.size());
    try {
      async = connection().async();
      for (String u : users) {
        sent.add(async.eval(RAISE, ScriptOutputType.INTEGER, new String[] {key(u)}, convId, Long.toString(last), "0",
            ttl()));
      }
    } catch (StoreUnavailableException e) {
      throw e;
    } catch (RuntimeException e) {
      throw new StoreUnavailableException("redis: " + e.getMessage(), e);
    }
    awaitAll(sent);
  }

  @Override
  public void drop(String user, String convId) {
    eval(DROP, user, convId, ttl());
  }

  @Override
  public void forget(Collection<String> users) {
    if (users.isEmpty()) {
      return;
    }
    try {
      connection().sync().del(users.stream().map(RedisBadges::key).toArray(String[]::new));
    } catch (StoreUnavailableException e) {
      throw e;
    } catch (RuntimeException e) {
      throw new StoreUnavailableException("redis: " + e.getMessage(), e);
    }
  }

  private static void awaitAll(List<RedisFuture<Long>> sent) {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
    for (RedisFuture<Long> f : sent) {
      try {
        f.get(Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new StoreUnavailableException("redis: interrupted", e);
      } catch (ExecutionException | TimeoutException e) {
        throw new StoreUnavailableException("redis: " + e, e);
      }
    }
  }

  @Override
  public void close() {
    if (conn != null) {
      conn.close();
    }
    client.shutdown(Duration.ZERO, Duration.ofSeconds(1));
  }
}
