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

  /**
   * Shared Lua: parse a field, and raise one conversation (KEYS[1]) by the max of each part, adjusting the total. A
   * field {@code L} is a conversation the user left (final review I1): raises leave it alone, only a join clears it.
   */
  private static final String LIB = """
      local function parse(v)
        if not v or v == 'L' then return 0, 0 end
        local i = string.find(v, ':', 1, true)
        return tonumber(string.sub(v, 1, i - 1)), tonumber(string.sub(v, i + 1))
      end
      local function raise(key, conv, last, read)
        local cur = redis.call('HGET', key, conv)
        if cur == 'L' then return end
        local l, r = parse(cur)
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
      local total = tonumber(redis.call('HGET', KEYS[1], '_total') or '0')
      if ARGV[1] == '' then return total end
      local cur = redis.call('HGET', KEYS[1], ARGV[1])
      if cur == 'L' then return total end
      if not cur then
        if tonumber(ARGV[2]) > 0 then return total + 1 end
        return total
      end
      raise(KEYS[1], ARGV[1], tonumber(ARGV[2]), 0)
      redis.call('PEXPIRE', KEYS[1], ARGV[4])
      return tonumber(redis.call('HGET', KEYS[1], '_total') or '0')
      """;

  /**
   * ARGV: conv, last, ttlMs. Nothing when the hash is missing; a left mark is cleared first; the membership version
   * {@code _mv} moves (Codex review of PR #10).
   */
  static final String JOIN = LIB + """
      if redis.call('EXISTS', KEYS[1]) == 0 then return -1 end
      redis.call('HINCRBY', KEYS[1], '_mv', 1)
      if redis.call('HGET', KEYS[1], ARGV[1]) == 'L' then redis.call('HDEL', KEYS[1], ARGV[1]) end
      raise(KEYS[1], ARGV[1], tonumber(ARGV[2]), tonumber(ARGV[2]))
      redis.call('PEXPIRE', KEYS[1], ARGV[3])
      return 0
      """;

  /** ARGV: conv, ttlMs. The field becomes a left mark (final review I1); the membership version moves. */
  static final String DROP = LIB + """
      if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
      redis.call('HINCRBY', KEYS[1], '_mv', 1)
      local l, r = parse(redis.call('HGET', KEYS[1], ARGV[1]))
      redis.call('HSET', KEYS[1], ARGV[1], 'L')
      redis.call('HINCRBY', KEYS[1], '_total', string.format('%d', -math.max(0, l - r)))
      redis.call('PEXPIRE', KEYS[1], ARGV[2])
      return 0
      """;

  /** ARGV: ttlMs. A placeholder that reads as stale when missing; answers the membership version. */
  static final String PREPARE = """
      redis.call('HSETNX', KEYS[1], '_built_at', '0')
      redis.call('PEXPIRE', KEYS[1], ARGV[1])
      return tonumber(redis.call('HGET', KEYS[1], '_mv') or '0')
      """;

  /**
   * ARGV: nowMs, ttlMs, expected membership version (-2 and nothing changed when it moved), then (conv, last, read)
   * per inbox row. Each row is merged by max (a left mark counts as
   * nothing), fields of conversations no longer in the inbox become left marks, the total is counted again. ARGV is
   * walked, never unpacked (10 000 rows).
   */
  static final String REBUILD = LIB + """
      if tonumber(redis.call('HGET', KEYS[1], '_mv') or '0') ~= tonumber(ARGV[3]) then return -2 end
      local keep = {}
      for i = 4, #ARGV, 3 do
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
          elseif all[i + 1] ~= 'L' then
            redis.call('HSET', KEYS[1], f, 'L')
          end
        end
      end
      redis.call('HSET', KEYS[1], '_total', string.format('%d', total), '_built_at', ARGV[1])
      redis.call('PEXPIRE', KEYS[1], ARGV[2])
      return total
      """;

  private final RedisClient client;
  private final Duration timeout;
  private volatile StatefulRedisConnection<String, String> conn;
  private volatile long nextConnectAt;

  public RedisBadges(ChatProperties.Redis cfg) {
    RedisURI uri = RedisURI.create(cfg.uri());
    uri.setTimeout(cfg.timeout());
    this.client = RedisClient.create(uri);
    this.timeout = cfg.timeout();
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

  /** 3a minor M3: the SHA-1 of each script, so calls send 40 bytes instead of the whole script. */
  private static final java.util.Map<String, String> SHAS = new java.util.concurrent.ConcurrentHashMap<>();

  static String sha(String script) {
    return SHAS.computeIfAbsent(script, x -> {
      try {
        return java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-1")
            .digest(x.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
      } catch (java.security.NoSuchAlgorithmException e) {
        throw new IllegalStateException(e);
      }
    });
  }

  /** EVALSHA; when Redis does not know the script (restart, SCRIPT FLUSH), EVAL, which also loads it. */
  private long eval(String script, String user, String... args) {
    try {
      String[] keys = {key(user)};
      Long v;
      try {
        v = connection().sync().evalsha(sha(script), ScriptOutputType.INTEGER, keys, args);
      } catch (io.lettuce.core.RedisNoScriptException e) {
        v = connection().sync().eval(script, ScriptOutputType.INTEGER, keys, args);
      }
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
  public long prepareRebuild(String user) {
    return eval(PREPARE, user, ttl());
  }

  @Override
  public boolean rebuild(String user, List<ChatStores.BadgeRow> snapshot, long nowMs, long version) {
    List<String> args = new ArrayList<>(3 + snapshot.size() * 3);
    args.add(Long.toString(nowMs));
    args.add(ttl());
    args.add(Long.toString(version));
    for (ChatStores.BadgeRow r : snapshot) {
      args.add(r.convId());
      args.add(Long.toString(r.lastSeq()));
      args.add(Long.toString(r.readSeq()));
    }
    return eval(REBUILD, user, args.toArray(String[]::new)) >= 0;
  }

  @Override
  public void raise(String user, String convId, long last, long read) {
    eval(RAISE, user, convId, Long.toString(last), Long.toString(read), ttl());
  }

  /**
   * One script per user, all written before any answer is awaited (Lettuce pipelines on one connection). Returns the
   * users whose script failed or did not answer within the one Redis timeout (3a minor M5).
   */
  @Override
  public List<String> raiseAll(List<String> users, String convId, long last) {
    RedisAsyncCommands<String, String> async;
    List<RedisFuture<Long>> sent = new ArrayList<>(users.size());
    try {
      async = connection().async();
      for (String u : users) {
        sent.add(async.evalsha(sha(RAISE), ScriptOutputType.INTEGER, new String[] {key(u)}, convId,
            Long.toString(last), "0", ttl()));
      }
    } catch (StoreUnavailableException e) {
      throw e;
    } catch (RuntimeException e) {
      throw new StoreUnavailableException("redis: " + e.getMessage(), e);
    }
    List<String> failed = new ArrayList<>();
    List<String> unknownScript = new ArrayList<>();
    long deadline = System.nanoTime() + timeout.toNanos();
    for (int i = 0; i < sent.size(); i++) {
      try {
        sent.get(i).get(Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        failed.addAll(users.subList(i, users.size()));
        break;
      } catch (ExecutionException e) {
        (e.getCause() instanceof io.lettuce.core.RedisNoScriptException ? unknownScript : failed).add(users.get(i));
      } catch (TimeoutException e) {
        failed.add(users.get(i));
      }
    }
    for (String u : unknownScript) {   // Redis forgot the script: EVAL once (which loads it), the rest by SHA again
      try {
        raise(u, convId, last, 0);
      } catch (RuntimeException e) {
        failed.add(u);
      }
    }
    return failed;
  }

  @Override
  public void join(String user, String convId, long last) {
    eval(JOIN, user, convId, Long.toString(last), ttl());
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

  @Override
  public void close() {
    if (conn != null) {
      conn.close();
    }
    client.shutdown(Duration.ZERO, Duration.ofSeconds(1));
  }
}
