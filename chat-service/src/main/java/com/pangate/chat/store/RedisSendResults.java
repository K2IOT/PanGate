package com.pangate.chat.store;

import com.pangate.chat.config.ChatProperties;
import com.pangate.chat.core.ChatStores.StoredMessage;
import com.pangate.chat.core.SendResults;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HTTP send results over Redis (chat spec §4.2): the sequencer writes {@code sendres:{conv}:{sender}:{clientMsgId}} =
 * {@code msgId|convSeq|sentAt} (TTL 5 min) and publishes {@code key\nvalue} on {@code sendres:<pod>}; the pod that took
 * the request waits on its channel. Any Redis failure means "no result": the request answers 202.
 */
public final class RedisSendResults implements SendResults, AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(RedisSendResults.class);
  static final Duration RESULT_TTL = Duration.ofMinutes(5);
  static final long RECONNECT_MS = 1_000;

  private final RedisClient client;
  private final String channel;
  private final Map<String, CompletableFuture<String>> waiting = new ConcurrentHashMap<>();
  private volatile StatefulRedisConnection<String, String> conn;
  private volatile StatefulRedisPubSubConnection<String, String> sub;
  private volatile long nextConnectAt;

  public RedisSendResults(ChatProperties.Redis cfg, String podId) {
    RedisURI uri = RedisURI.create(cfg.uri());
    uri.setTimeout(cfg.timeout());
    this.client = RedisClient.create(uri);
    this.channel = "sendres:" + podId;
  }

  public static String replyTo(String podId) {
    return "http:" + podId;
  }

  /** Both connections, subscribed; false while Redis is unreachable (a connect is tried at most once a second). */
  private boolean ready() {
    if (conn != null && sub != null) {
      return true;
    }
    synchronized (this) {
      if ((conn == null || sub == null) && System.currentTimeMillis() >= nextConnectAt) {
        try {
          if (conn == null) {
            conn = client.connect();
          }
          if (sub == null) {
            StatefulRedisPubSubConnection<String, String> s = client.connectPubSub();
            s.addListener(new RedisPubSubAdapter<>() {
              @Override
              public void message(String ch, String message) {
                int nl = message.indexOf('\n');
                if (nl > 0) {
                  CompletableFuture<String> f = waiting.get(message.substring(0, nl));
                  if (f != null) {
                    f.complete(message.substring(nl + 1));
                  }
                }
              }
            });
            s.sync().subscribe(channel);
            sub = s;
          }
        } catch (RuntimeException e) {
          nextConnectAt = System.currentTimeMillis() + RECONNECT_MS;
          log.debug("redis not connected: {}", e.toString());
        }
      }
      return conn != null && sub != null;
    }
  }

  static String value(StoredMessage m) {
    return m.msgId() + "|" + m.convSeq() + "|" + m.sentAtMs();
  }

  static StoredMessage parse(String key, String v) {
    String[] p = v.split("\\|", 3);
    return new StoredMessage("", Long.parseLong(p[1]), p[0], "", "", "", "", List.of(), Long.parseLong(p[2]));
  }

  @Override
  public void publish(String replyTo, StoredMessage m) {
    if (replyTo == null || !replyTo.startsWith("http:")) {
      return;
    }
    try {
      if (conn == null && !ready()) {
        return;
      }
      String key = SendResults.key(m.convId(), m.senderId(), m.clientMsgId());
      String v = value(m);
      conn.sync().set(key, v, SetArgs.Builder.px(RESULT_TTL.toMillis()));
      conn.sync().publish("sendres:" + replyTo.substring("http:".length()), key + "\n" + v);
    } catch (RuntimeException e) {
      log.debug("send result not published: {}", e.toString());
    }
  }

  @Override
  public Optional<StoredMessage> await(String key, Duration wait) {
    if (!ready()) {
      return Optional.empty();
    }
    CompletableFuture<String> f = new CompletableFuture<>();
    waiting.put(key, f);
    try {
      String now = conn.sync().get(key);   // after registering: a result published in between is not missed
      if (now != null) {
        return Optional.of(parse(key, now));
      }
      return Optional.of(parse(key, f.get(wait.toNanos(), TimeUnit.NANOSECONDS)));
    } catch (TimeoutException | ExecutionException | RuntimeException e) {
      return Optional.empty();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return Optional.empty();
    } finally {
      waiting.remove(key, f);
    }
  }

  @Override
  public void close() {
    if (sub != null) {
      sub.close();
    }
    if (conn != null) {
      conn.close();
    }
    client.shutdown(Duration.ZERO, Duration.ofSeconds(1));
  }
}
