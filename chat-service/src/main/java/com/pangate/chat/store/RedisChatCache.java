package com.pangate.chat.store;

import com.pangate.chat.config.ChatProperties;
import com.pangate.chat.core.ChatStores;
import com.pangate.chat.core.ChatStores.Member;
import com.pangate.chat.core.ChatStores.Role;
import com.pangate.chat.core.ChatStores.StoredMessage;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Redis in front of ScyllaDB (chat spec §5): member lists ({@code members:{conv}}, TTL 60 s) and send dedup
 * ({@code dedup:{conv}:{sender}:{clientMsgId}}, TTL 24 h). A cache: every Redis failure reads as a miss and a write
 * is dropped, so a Redis outage only costs ScyllaDB reads.
 */
public final class RedisChatCache implements ChatStores.MemberCache, ChatStores.DedupStore, AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(RedisChatCache.class);
  static final Duration DEDUP_TTL = Duration.ofHours(24);
  static final long RECONNECT_MS = 1_000;

  /** Written by an eviction for {@link #EVICT_HOLD}: a list read before the change cannot be cached right after it. */
  static final String EVICTED = "~evicted~";
  static final Duration EVICT_HOLD = Duration.ofSeconds(2);

  private final RedisClient client;
  private final Duration memberTtl;
  private volatile StatefulRedisConnection<String, String> conn;
  private volatile long nextConnectAt;

  public RedisChatCache(ChatProperties.Redis cfg) {
    RedisURI uri = RedisURI.create(cfg.uri());
    uri.setTimeout(cfg.timeout());
    this.client = RedisClient.create(uri);
    this.memberTtl = cfg.memberCacheTtl();
  }

  /** Lazily connected; a failed connect is retried at most once a second. */
  private RedisCommands<String, String> redis() {
    StatefulRedisConnection<String, String> c = conn;
    if (c != null) {
      return c.sync();
    }
    synchronized (this) {
      if (conn == null && System.currentTimeMillis() >= nextConnectAt) {
        try {
          conn = client.connect();
        } catch (RuntimeException e) {
          nextConnectAt = System.currentTimeMillis() + RECONNECT_MS;
          log.debug("redis not connected: {}", e.toString());
        }
      }
      return conn == null ? null : conn.sync();
    }
  }

  public Optional<List<Member>> members(String convId) {
    try {
      RedisCommands<String, String> r = redis();
      String v = r == null ? null : r.get("members:{" + convId + "}");
      if (v == null || v.equals(EVICTED)) {
        return Optional.empty();
      }
      List<Member> out = new ArrayList<>();
      for (String line : v.split("\n")) {
        if (!line.isEmpty()) {
          int eq = line.lastIndexOf('=');
          out.add(new Member(line.substring(0, eq), Role.valueOf(line.substring(eq + 1))));
        }
      }
      return Optional.of(out);
    } catch (RuntimeException e) {
      return Optional.empty();
    }
  }

  @Override
  public Optional<List<Member>> cachedMembers(String convId) {
    return members(convId);
  }

  @Override
  public void putMembers(String convId, List<Member> members) {
    StringBuilder b = new StringBuilder();
    members.forEach(m -> b.append(m.userId()).append('=').append(m.role().name()).append('\n'));
    try {
      RedisCommands<String, String> r = redis();
      if (r != null) {
        r.set("members:{" + convId + "}", b.toString(),
            io.lettuce.core.SetArgs.Builder.nx().px(memberTtl.toMillis()));   // never over an eviction marker
      }
    } catch (RuntimeException e) {
      log.debug("member cache not written: {}", e.toString());
    }
  }

  /**
   * Replaces the list with a short-lived marker. False when Redis answered with an error (the old list may stay for
   * its TTL). Not connected at all counts as evicted: nothing can be read from it either (chat spec §8, Redis down).
   */
  @Override
  public boolean evictMembers(String convId) {
    RedisCommands<String, String> r = redis();
    if (r == null) {
      return true;
    }
    try {
      r.psetex("members:{" + convId + "}", EVICT_HOLD.toMillis(), EVICTED);
      return true;
    } catch (RuntimeException e) {
      log.warn("member cache not evicted: {}", e.toString());
      return false;
    }
  }

  static String dedupKey(String convId, String senderId, String clientMsgId) {
    return "dedup:{" + convId + "}:" + senderId + ":" + clientMsgId;
  }

  /** Same partial message as {@code DedupStore.find}. */
  public Optional<StoredMessage> dedup(String convId, String senderId, String clientMsgId) {
    try {
      RedisCommands<String, String> r = redis();
      String v = r == null ? null : r.get(dedupKey(convId, senderId, clientMsgId));
      if (v == null) {
        return Optional.empty();
      }
      String[] p = v.split("\\|", 3);
      return Optional.of(new StoredMessage(convId, Long.parseLong(p[1]), p[0], senderId, clientMsgId, "", "",
          List.of(), Long.parseLong(p[2])));
    } catch (RuntimeException e) {
      return Optional.empty();
    }
  }

  @Override
  public Optional<StoredMessage> find(String convId, String senderId, String clientMsgId) {
    return dedup(convId, senderId, clientMsgId);
  }

  @Override
  public void put(StoredMessage m) {
    putDedup(m);
  }

  public void putDedup(StoredMessage m) {
    try {
      RedisCommands<String, String> r = redis();
      if (r != null) {
        r.psetex(dedupKey(m.convId(), m.senderId(), m.clientMsgId()), DEDUP_TTL.toMillis(),
            m.msgId() + "|" + m.convSeq() + "|" + m.sentAtMs());
      }
    } catch (RuntimeException e) {
      log.debug("dedup cache not written: {}", e.toString());
    }
  }

  @Override
  public void close() {
    StatefulRedisConnection<String, String> c = conn;
    if (c != null) {
      c.close();
    }
    client.shutdown(Duration.ZERO, Duration.ofSeconds(1));
  }
}
