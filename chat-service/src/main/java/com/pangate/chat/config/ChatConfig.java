package com.pangate.chat.config;

import com.datastax.oss.driver.api.core.CqlSession;
import com.pangate.chat.auth.ConnectTokenVerifier;
import com.pangate.chat.auth.HttpJwksSource;
import com.pangate.chat.auth.RevocationListener;
import com.pangate.chat.auth.Revocations;
import com.pangate.chat.core.ChannelSubscriptions;
import com.pangate.chat.core.ChatStores;
import com.pangate.chat.core.ChatStores.StoredMessage;
import com.pangate.chat.core.Conversations;
import com.pangate.chat.core.Ingest;
import com.pangate.chat.core.UserDirectory;
import com.pangate.chat.core.AccountRemoval;
import com.pangate.chat.kafka.AccountsListener;
import com.pangate.contracts.testkit.AccountTopics;
import com.pangate.chat.core.SyncService;
import com.pangate.chat.core.MemberLookup;
import com.pangate.chat.core.Notifier;
import com.pangate.chat.core.PushNotifier;
import com.pangate.chat.core.Sequencer;
import com.pangate.chat.http.ApiMetrics;
import com.pangate.chat.http.AuthFilter;
import com.pangate.chat.http.PortRoutingFilter;
import com.pangate.chat.kafka.CommandProducer;
import com.pangate.chat.grpc.PushApiClient;
import com.pangate.chat.kafka.EventProducer;
import com.pangate.chat.kafka.FanOutLoop;
import com.pangate.chat.kafka.SequencerLoop;
import com.pangate.chat.kafka.UpstreamCommandListener;
import com.pangate.chat.store.RedisChatCache;
import com.pangate.chat.store.RedisSendResults;
import com.pangate.chat.store.SchemaMigrator;
import com.pangate.chat.store.ScyllaChatStore;
import com.pangate.chat.store.ScyllaSession;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Wiring of chat-service (chat spec §2). Background parts start after the context and stop in reverse order. */
@Configuration(proxyBeanMethods = false)
public class ChatConfig {

  private static final Logger log = LoggerFactory.getLogger(ChatConfig.class);
  static final String CONTROL_TOPIC = "gw.control";

  /** Closes each part in order; one that fails is logged and the rest are still closed. */
  static void closeInOrder(AutoCloseable... parts) {
    for (AutoCloseable part : parts) {
      try {
        part.close();
      } catch (Exception e) {
        log.warn("closing {} failed: {}", part, e.toString());
      }
    }
  }

  static long monoMs() {
    return System.nanoTime() / 1_000_000;
  }

  @Bean(destroyMethod = "")
  CqlSession cqlSession(ChatProperties p) {
    CqlSession s = ScyllaSession.connect(p.scylla());
    SchemaMigrator.migrate(s, p.scylla().keyspace());
    return s;
  }

  @Bean
  ScyllaChatStore scyllaChatStore(CqlSession s, ChatProperties p) {
    return new ScyllaChatStore(s, p.scylla().keyspace());
  }

  @Bean(destroyMethod = "")
  RedisChatCache redisChatCache(ChatProperties p) {
    return new RedisChatCache(p.redis());
  }

  @Bean(destroyMethod = "")
  CommandProducer commandProducer(ChatProperties p) {
    return CommandProducer.create(p.kafka().bootstrap(), p.kafka().produceTimeout(), p.kafka().commandsTopic(),
        p.kafka().dlqTopic(), p.kafka().receiptsTopic());
  }

  /** CH3 spec §3.3: delivered / read marks into chat.receipts. */
  @Bean
  com.pangate.chat.core.StateIngest stateIngest(ScyllaChatStore store, RedisChatCache cache, CommandProducer producer,
      ChatProperties p, MeterRegistry meters, com.pangate.chat.core.TypingRelay typing,
      com.pangate.chat.core.PresenceWatch watch, RecentCount watchers) {
    return new com.pangate.chat.core.StateIngest(new com.pangate.chat.core.MarkValidator(
        new MemberLookup(store, cache), new com.pangate.chat.core.LastSeqCache(store, () -> System.nanoTime() / 1_000_000),
        new com.pangate.chat.core.TokenBucketLimiter(p.state().markRate(), p.state().markBurst(), System::nanoTime),
        reason -> meters.counter("chat.receipts.rejected", "reason", reason).increment()), producer,
        System::currentTimeMillis, typing, r -> {
          if (r.equals("relayed")) {
            meters.counter("chat.typing.relayed").increment();
          } else {
            meters.counter("chat.typing.dropped", "reason", r).increment();
          }
        }, watch, r -> {
          if (r.equals("unavailable")) {
            meters.counter("chat.presence.unavailable").increment();
          } else {
            watchers.add(r);
          }
        });
  }

  /** chat.presence.watchers: watch calls this pod took in the last watch TTL (an estimate, spec §9). */
  @Bean
  com.pangate.chat.config.ChatConfig.RecentCount presenceWatchers(MeterRegistry meters, ChatProperties p) {
    RecentCount c = new RecentCount(p.state().watchTtl());
    meters.gauge("chat.presence.watchers", c, RecentCount::size);
    return c;
  }

  /** Events within a sliding window, for estimated gauges. */
  public static final class RecentCount {
    private final java.util.concurrent.ConcurrentLinkedDeque<Long> at = new java.util.concurrent.ConcurrentLinkedDeque<>();
    private final long windowMs;

    RecentCount(java.time.Duration window) {
      this.windowMs = window.toMillis();
    }

    void add(String ignored) {
      at.add(System.currentTimeMillis());
      if (at.size() > 1_000_000) {
        at.pollFirst();
      }
    }

    double size() {
      long cut = System.currentTimeMillis() - windowMs;
      while (!at.isEmpty() && at.peekFirst() < cut) {
        at.pollFirst();
      }
      return at.size();
    }
  }

  /** CH3 spec §6.3. */
  @Bean
  com.pangate.chat.core.PresenceWatch presenceWatch(ScyllaChatStore store, RedisChatCache cache,
      com.pangate.chat.store.RedisPresence presence, com.pangate.chat.core.PrivacyCache privacy, PushApiClient pusher,
      ChatProperties p, MeterRegistry meters) {
    com.pangate.chat.core.Pusher counted = r -> {
      meters.counter("chat.presence.pushes").increment();
      return pusher.send(r);
    };
    return new com.pangate.chat.core.PresenceWatch(store, new MemberLookup(store, cache), presence, presence, store,
        privacy, counted, System::currentTimeMillis, p.state().watchTtl(), p.state().watchMax());
  }

  /** CH3 spec §6.3: transitions to the devices watching; Redis down is counted, not thrown into the tracker. */
  @Bean
  java.util.function.Consumer<com.pangate.chat.core.PresenceTracker.Transition> presenceFanOut(
      com.pangate.chat.store.RedisPresence presence, ScyllaChatStore store, com.pangate.chat.core.PrivacyCache privacy,
      PushApiClient pusher, MeterRegistry meters) {
    com.pangate.chat.core.PresenceFanOut f = new com.pangate.chat.core.PresenceFanOut(presence, presence, store,
        privacy, pusher, System::currentTimeMillis);
    return t -> {
      try {
        meters.counter("chat.presence.pushes").increment(f.on(t));
      } catch (com.pangate.chat.core.StoreUnavailableException e) {
        meters.counter("chat.presence.unavailable").increment();
      }
    };
  }

  /** CH3 spec §5: typing, relayed directly (the open-conversation index comes with presence watch). */
  @Bean
  com.pangate.chat.core.TypingRelay typingRelay(ScyllaChatStore store, RedisChatCache cache, PushApiClient pusher,
      ChatProperties p, com.pangate.chat.store.RedisPresence presence) {
    return new com.pangate.chat.core.TypingRelay(store, new MemberLookup(store, cache), pusher,
        presence, () -> System.nanoTime() / 1_000_000,
        p.state().listedReceiptsMax());
  }

  @Bean
  Ingest ingest(ScyllaChatStore store, RedisChatCache cache, CommandProducer producer, ChatProperties p,
      MeterRegistry meters, UserDirectory users) {
    return new Ingest(store, store, cache, producer, p.limits(), System::nanoTime, System::currentTimeMillis,
        reason -> meters.counter("chat.send.rejected", "reason", reason).increment(), users);
  }

  /** {@code http:<pod>}: HOSTNAME (the pod name) plus a random suffix, so two processes on one host never share it. */
  @Bean
  String httpReplyTo() {
    String host = System.getenv().getOrDefault("HOSTNAME", "chat");
    return RedisSendResults.replyTo(host + "-" + UUID.randomUUID().toString().substring(0, 8));
  }

  @Bean
  Duration httpSendWait(ChatProperties p) {
    return p.http().sendWait();
  }

  @Bean(destroyMethod = "")
  RedisSendResults redisSendResults(ChatProperties p, @Qualifier("httpReplyTo") String replyTo) {
    return new RedisSendResults(p.redis(), replyTo.substring("http:".length()));
  }

  /** Earlier sends with a key: Redis first, then ScyllaDB (chat spec §4.4 step 1). */
  @Bean
  ChatStores.DedupStore priorSends(RedisChatCache cache, ScyllaChatStore store) {
    return new ChatStores.DedupStore() {
      @Override
      public Optional<StoredMessage> find(String convId, String senderId, String clientMsgId) {
        Optional<StoredMessage> hit = cache.find(convId, senderId, clientMsgId);
        return hit.isPresent() ? hit : store.find(convId, senderId, clientMsgId);
      }

      @Override
      public void put(StoredMessage m) {
        store.put(m);
        cache.put(m);
      }
    };
  }

  @Bean
  Conversations conversations(ScyllaChatStore store, RedisChatCache cache, ChatProperties p,
      ChannelSubscriptions channels, UserDirectory users) {
    return new Conversations(store, store, new MemberLookup(store, cache), p.limits(), channels::revoke, users);
  }

  /** account spec §8: users known from account.users. */
  @Bean
  UserDirectory userDirectory(ScyllaChatStore store) {
    return new UserDirectory(store);
  }

  @Bean
  AccountRemoval accountRemoval(ScyllaChatStore store, Conversations conversations) {
    return new AccountRemoval(store, conversations);
  }

  @Bean(destroyMethod = "")
  AccountsListener accountsListener(ChatProperties p, ScyllaChatStore store, AccountRemoval removal,
      UserDirectory users) {
    return new AccountsListener(AccountsListener.consumer(p.kafka().bootstrap(), AccountsListener.GROUP),
        AccountTopics.USERS, store, removal::removed, users::forget);
  }

  @Bean
  SyncService syncService(ScyllaChatStore store) {
    return new SyncService(store, store, store);
  }

  @Bean
  org.springframework.web.servlet.config.annotation.WebMvcConfigurer apiMetrics(MeterRegistry meters) {
    return new org.springframework.web.servlet.config.annotation.WebMvcConfigurer() {
      @Override
      public void addInterceptors(org.springframework.web.servlet.config.annotation.InterceptorRegistry r) {
        r.addInterceptor(new ApiMetrics(meters));
      }
    };
  }

  @Bean
  Sequencer sequencer(ScyllaChatStore store, RedisChatCache cache) {
    return new Sequencer(store, store, store, cache, System::currentTimeMillis);
  }

  @Bean(destroyMethod = "")
  EventProducer eventProducer(ChatProperties p) {
    return EventProducer.create(p.kafka().bootstrap(), p.kafka().eventsTopic(), p.kafka().produceTimeout());
  }

  @Bean(destroyMethod = "")
  SequencerLoop sequencerLoop(ChatProperties p, Sequencer sequencer, EventProducer events, RedisSendResults results,
      CommandProducer producer, MeterRegistry meters) {
    return new SequencerLoop(SequencerLoop.consumer(p.kafka().bootstrap(), SequencerLoop.GROUP),
        p.kafka().commandsTopic(), sequencer, events, results, producer, meters);
  }

  @Bean(destroyMethod = "")
  PushApiClient pushApiClient(ChatProperties p) {
    return PushApiClient.create(p.pushApi());
  }

  @Bean
  Notifier notifier(PushApiClient pusher, MeterRegistry meters) {
    return new PushNotifier(pusher, result -> meters.counter("chat.rejected.push", "result", result).increment());
  }

  @Bean(destroyMethod = "")
  FanOutLoop fanOutLoop(ChatProperties p, ScyllaChatStore store, RedisChatCache cache, PushApiClient pusher,
      MeterRegistry meters) {
    return new FanOutLoop(FanOutLoop.consumer(p.kafka().bootstrap(), FanOutLoop.GROUP), p.kafka().eventsTopic(),
        new MemberLookup(store, cache), store, store, pusher, meters);
  }

  @Bean(destroyMethod = "")
  UpstreamCommandListener upstreamCommandListener(ChatProperties p, Ingest ingest, Notifier notifier,
      CommandProducer producer, MeterRegistry meters, ChannelSubscriptions channels,
      com.pangate.chat.core.StateIngest state) {
    return new UpstreamCommandListener(UpstreamCommandListener.consumer(p.kafka().bootstrap()),
        p.kafka().upstreamTopic(), ingest, notifier, producer,
        outcome -> meters.counter("chat.ingest.records", "result", outcome).increment(), channels, state);
  }

  @Bean
  ChannelSubscriptions channelSubscriptions(ScyllaChatStore store, PushApiClient pusher, MeterRegistry meters) {
    return new ChannelSubscriptions(store, store, pusher,
        result -> meters.counter("chat.channel.control", "result", result).increment());
  }

  @Bean
  Revocations revocations() {
    return new Revocations(System::currentTimeMillis);
  }

  @Bean(destroyMethod = "")
  RevocationListener revocationListener(ChatProperties p, Revocations revocations, MeterRegistry meters) {
    KafkaConsumer<String, byte[]> c = new KafkaConsumer<>(Map.of(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, p.kafka().bootstrap(),
        ConsumerConfig.CLIENT_ID_CONFIG, "chat-control-" + UUID.randomUUID(),
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false), new StringDeserializer(), new ByteArrayDeserializer());
    return new RevocationListener(c, CONTROL_TOPIC, revocations,
        () -> meters.counter("chat.control.records", "result", "malformed").increment());
  }

  @Bean
  ConnectTokenVerifier connectTokenVerifier(ChatProperties p, Revocations revocations) {
    return new ConnectTokenVerifier(new HttpJwksSource(p.auth().jwksUri()), p.auth().issuer(), p.auth().audience(),
        revocations, System::currentTimeMillis, ChatConfig::monoMs);
  }

  @Bean
  FilterRegistrationBean<PortRoutingFilter> portRoutingFilter(AdminConnector admin) {
    FilterRegistrationBean<PortRoutingFilter> b = new FilterRegistrationBean<>(new PortRoutingFilter(admin));
    b.setOrder(1);
    return b;
  }

  @Bean
  FilterRegistrationBean<AuthFilter> authFilter(ConnectTokenVerifier verifier) {
    FilterRegistrationBean<AuthFilter> b = new FilterRegistrationBean<>(new AuthFilter(verifier::verify));
    b.setOrder(2);
    return b;
  }

  /** Bean name {@code chatHealthIndicator} → health contributor {@code chat} (readiness group). */
  @Bean
  ChatHealth chatHealthIndicator(RevocationListener revocations, ConnectTokenVerifier verifier) {
    return new ChatHealth(revocations::caughtUp, verifier::loaded, ChatConfig::monoMs);
  }

  /** CH3 spec §8: privacy settings cached 60 s per pod. */
  @Bean
  com.pangate.chat.core.PrivacyCache privacyCache(ScyllaChatStore store) {
    return new com.pangate.chat.core.PrivacyCache(store, () -> System.nanoTime() / 1_000_000);
  }

  /** CH3 spec §4, §7.2, §3.4 lookups. */
  @Bean
  com.pangate.chat.core.StateService stateService(ScyllaChatStore store, RedisChatCache cache,
      com.pangate.chat.core.PrivacyCache privacy, ChatProperties p) {
    return new com.pangate.chat.core.StateService(store, new MemberLookup(store, cache), store, store, store, privacy,
        p.state().listedReceiptsMax(), () -> System.nanoTime() / 1_000_000, System::currentTimeMillis);
  }

  /** CH3 spec §6.1: connection sets in Redis. */
  @Bean(destroyMethod = "")
  com.pangate.chat.store.RedisPresence redisPresence(ChatProperties p) {
    return new com.pangate.chat.store.RedisPresence(p.redis());
  }

  /** Transitions are counted, then fanned out to watchers (presenceFanOut). */
  @Bean
  com.pangate.chat.core.PresenceTracker presenceTracker(com.pangate.chat.store.RedisPresence presence,
      ScyllaChatStore store, ChatProperties p, MeterRegistry meters,
      org.springframework.beans.factory.ObjectProvider<java.util.function.Consumer<
          com.pangate.chat.core.PresenceTracker.Transition>> fanOut) {
    return new com.pangate.chat.core.PresenceTracker(presence, store, t -> {
      meters.counter("chat.presence.transitions", "to", t.online() ? "online" : "offline").increment();
      fanOut.ifAvailable(f -> f.accept(t));
    }, System::currentTimeMillis, p.state().offlineGrace());
  }

  @Bean(destroyMethod = "")
  com.pangate.chat.kafka.LifecycleListener lifecycleListener(ChatProperties p,
      com.pangate.chat.core.PresenceTracker tracker, MeterRegistry meters) {
    return new com.pangate.chat.kafka.LifecycleListener(com.pangate.chat.kafka.LifecycleListener.consumer(
        p.kafka().bootstrap(), com.pangate.chat.kafka.LifecycleListener.GROUP),
        com.pangate.contracts.testkit.PushTopics.lifecycle(p.cell()).name(), tracker, meters);
  }

  /** CH3 spec §3.4: chat.receipts → marks → receipt pushes. */
  @Bean(destroyMethod = "")
  com.pangate.chat.kafka.ReceiptWorkerLoop receiptWorkerLoop(ChatProperties p, ScyllaChatStore store,
      RedisChatCache cache, com.pangate.chat.core.PrivacyCache privacy, PushApiClient pusher, MeterRegistry meters) {
    return new com.pangate.chat.kafka.ReceiptWorkerLoop(com.pangate.chat.kafka.ReceiptWorkerLoop.consumer(
        p.kafka().bootstrap(), com.pangate.chat.kafka.ReceiptWorkerLoop.GROUP), p.kafka().receiptsTopic(), store,
        new MemberLookup(store, cache), store, privacy, pusher, p.state(), meters);
  }

  @Bean
  SmartLifecycle chatBackground(CqlSession cql, ChatHealth health, ConnectTokenVerifier verifier,
      RevocationListener revocationListener, UpstreamCommandListener upstream, CommandProducer producer,
      RedisChatCache redis, MeterRegistry meters, Revocations revocations, SequencerLoop sequencerLoop,
      EventProducer events, RedisSendResults results, FanOutLoop fanOut, PushApiClient pusher,
      AccountsListener accounts, com.pangate.chat.kafka.ReceiptWorkerLoop receipts,
      com.pangate.chat.kafka.LifecycleListener lifecycle, com.pangate.chat.store.RedisPresence presence) {
    return new SmartLifecycle() {
      private volatile boolean running;
      private ScheduledExecutorService timers;

      @Override
      public void start() {
        timers = Executors.newScheduledThreadPool(1, Thread.ofPlatform().daemon().name("chat-timer").factory());
        timers.scheduleWithFixedDelay(() -> verifier.refresh(true), 0, 300, TimeUnit.SECONDS);
        timers.scheduleWithFixedDelay(() -> {
          if (!verifier.loaded()) {
            verifier.refresh(false);   // until the first JWKS arrives (readiness)
          }
        }, 2, 2, TimeUnit.SECONDS);
        timers.scheduleWithFixedDelay(() -> {
          try {
            cql.execute("SELECT release_version FROM system.local");
            health.scyllaAnswered();
          } catch (RuntimeException e) {
            log.warn("ScyllaDB probe failed: {}", e.toString());
          }
        }, 0, 2, TimeUnit.SECONDS);
        meters.gauge("chat.revocations.active", revocations, Revocations::size);
        revocationListener.start();
        sequencerLoop.start();
        fanOut.start();
        receipts.start();
        lifecycle.start();
        accounts.start();
        upstream.start();
        running = true;
      }

      @Override
      public void stop() {
        running = false;
        closeInOrder(upstream, accounts, lifecycle, sequencerLoop, fanOut, receipts, pusher, revocationListener,
            producer, events, results, timers::shutdownNow, presence, redis, cql);
      }

      @Override
      public boolean isRunning() {
        return running;
      }

      /** Below the web server phase: starts before it serves, stops after it drained. */
      @Override
      public int getPhase() {
        return 0;
      }
    };
  }
}
