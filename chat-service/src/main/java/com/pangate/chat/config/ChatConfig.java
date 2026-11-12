package com.pangate.chat.config;

import com.datastax.oss.driver.api.core.CqlSession;
import com.pangate.chat.auth.ConnectTokenVerifier;
import com.pangate.chat.auth.HttpJwksSource;
import com.pangate.chat.auth.RevocationListener;
import com.pangate.chat.auth.Revocations;
import com.pangate.chat.core.ChatStores;
import com.pangate.chat.core.ChatStores.StoredMessage;
import com.pangate.chat.core.Ingest;
import com.pangate.chat.core.MemberLookup;
import com.pangate.chat.core.Notifier;
import com.pangate.chat.core.PushNotifier;
import com.pangate.chat.core.Sequencer;
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
    return CommandProducer.create(p.kafka().bootstrap(), p.kafka().produceTimeout());
  }

  @Bean
  Ingest ingest(ScyllaChatStore store, RedisChatCache cache, CommandProducer producer, ChatProperties p,
      MeterRegistry meters) {
    return new Ingest(store, store, cache, producer, p.limits(), System::nanoTime, System::currentTimeMillis,
        reason -> meters.counter("chat.send.rejected", "reason", reason).increment());
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
    return new FanOutLoop(SequencerLoop.consumer(p.kafka().bootstrap(), FanOutLoop.GROUP), p.kafka().eventsTopic(),
        new MemberLookup(store, cache), store, pusher, meters);
  }

  @Bean(destroyMethod = "")
  UpstreamCommandListener upstreamCommandListener(ChatProperties p, Ingest ingest, Notifier notifier,
      CommandProducer producer, MeterRegistry meters) {
    return new UpstreamCommandListener(UpstreamCommandListener.consumer(p.kafka().bootstrap()),
        p.kafka().upstreamTopic(), ingest, notifier, producer,
        outcome -> meters.counter("chat.ingest.records", "result", outcome).increment());
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

  @Bean
  SmartLifecycle chatBackground(CqlSession cql, ChatHealth health, ConnectTokenVerifier verifier,
      RevocationListener revocationListener, UpstreamCommandListener upstream, CommandProducer producer,
      RedisChatCache redis, MeterRegistry meters, Revocations revocations, SequencerLoop sequencerLoop,
      EventProducer events, RedisSendResults results, FanOutLoop fanOut, PushApiClient pusher) {
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
        upstream.start();
        running = true;
      }

      @Override
      public void stop() {
        running = false;
        upstream.close();
        sequencerLoop.close();
        fanOut.close();
        pusher.close();
        revocationListener.close();
        producer.close();
        events.close();
        results.close();
        timers.shutdownNow();
        redis.close();
        cql.close();
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
