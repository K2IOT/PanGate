package com.pangate.pushapi.config;

import com.pangate.pushapi.auth.ApiTokenVerifier;
import com.pangate.pushapi.auth.HttpJwksSource;
import com.pangate.pushapi.core.ControlState;
import com.pangate.pushapi.core.Idempotency;
import com.pangate.pushapi.core.KeyedSerialExecutor;
import com.pangate.pushapi.core.MessageStatus;
import com.pangate.pushapi.core.PushAcceptor;
import com.pangate.pushapi.core.RateLimiter;
import com.pangate.pushapi.core.RecordBuilder;
import com.pangate.pushapi.core.SeqAllocator;
import com.pangate.pushapi.core.TenantPolicy;
import com.pangate.pushapi.http.ApiAuthFilter;
import com.pangate.pushapi.http.BodyLimitFilter;
import com.pangate.pushapi.http.PortRoutingFilter;
import com.pangate.pushapi.kafka.ControlListener;
import com.pangate.pushapi.kafka.KafkaRecordProducer;
import com.pangate.pushapi.kafka.StatusListener;
import com.pangate.pushapi.store.RedisIdemStore;
import com.pangate.pushapi.store.RedisMessageStatus;
import com.pangate.pushapi.store.RedisSeqEpochs;
import com.pangate.pushapi.store.RedisSeqStore;
import com.pangate.pushapi.store.RedisState;
import com.pangate.pushapi.store.SeqEpochWatcher;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Wires the Spring-free core to Redis, Kafka and the HTTP filters (push-api spec §1). */
@Configuration(proxyBeanMethods = false)
public class PushApiConfig {

  public static final String CONTROL_TOPIC = "gw.control";

  static long monoMs() {
    return System.nanoTime() / 1_000_000;
  }

  @Bean(destroyMethod = "")
  RedisState redisState(PushApiProperties p) {
    return RedisState.connect(p.redis());
  }

  @Bean
  RedisSeqEpochs seqEpochs(RedisState redis) {
    return new RedisSeqEpochs(redis);
  }

  @Bean(destroyMethod = "")
  SeqEpochWatcher seqEpochWatcher(RedisState redis, RedisSeqEpochs epochs, MeterRegistry meters) {
    return new SeqEpochWatcher(redis, epochs,
        reason -> meters.counter("pushapi.seqepoch.bumps", "reason", reason).increment());
  }

  @Bean
  ControlState controlState(PushApiProperties p) {
    PushApiProperties.Limits l = p.limits();
    return new ControlState(p.cell(), new TenantPolicy(false, true, l.pushRate(), l.pushBurst(), l.broadcastRate()),
        System::currentTimeMillis);
  }

  @Bean
  MessageStatus messageStatus(RedisState redis) {
    return new RedisMessageStatus(redis, System::currentTimeMillis);
  }

  @Bean(destroyMethod = "")
  KafkaRecordProducer recordProducer(PushApiProperties p) {
    return KafkaRecordProducer.create(p.kafka().bootstrap(), p.cell(), p.kafka().produceTimeout());
  }

  @Bean
  PushAcceptor pushAcceptor(PushApiProperties p, RedisState redis, RedisSeqEpochs epochs, SeqEpochWatcher watcher,
      ControlState control, KafkaRecordProducer producer, MessageStatus status, MeterRegistry meters) {
    SeqAllocator seq = new SeqAllocator(new RedisSeqStore(redis), epochs, () -> {
      meters.counter("pushapi.seq.regressions").increment();
      watcher.checkNow();
    });
    Idempotency idem = new Idempotency(new RedisIdemStore(redis), p.idempotency().ttl(), p.idempotency().pendingTtl(),
        p.idempotency().waitFor(), d -> Thread.sleep(d), PushApiConfig::monoMs);
    return new PushAcceptor(seq, new RecordBuilder(p.cell(), System::currentTimeMillis, p.ttl().defaultTtl()), idem,
        control, new RateLimiter(System::nanoTime), producer, status, System::currentTimeMillis);
  }

  @Bean(destroyMethod = "close")
  ExecutorService pushWorkers() {
    return Executors.newVirtualThreadPerTaskExecutor();
  }

  @Bean
  KeyedSerialExecutor keyedSerialExecutor(ExecutorService pushWorkers) {
    return new KeyedSerialExecutor(pushWorkers);
  }

  @Bean
  ApiTokenVerifier apiTokenVerifier(PushApiProperties p, ControlState control) {
    return new ApiTokenVerifier(new HttpJwksSource(p.auth().jwksUri()), p.auth().issuer(), p.auth().audience(),
        control, System::currentTimeMillis, PushApiConfig::monoMs);
  }

  @Bean(destroyMethod = "")
  ControlListener controlListener(PushApiProperties p, ControlState control, MeterRegistry meters) {
    KafkaConsumer<String, byte[]> c = new KafkaConsumer<>(Map.of(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, p.kafka().bootstrap(),
        ConsumerConfig.CLIENT_ID_CONFIG, "push-api-control-" + UUID.randomUUID(),
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false), new StringDeserializer(), new ByteArrayDeserializer());
    return new ControlListener(c, CONTROL_TOPIC, control,
        () -> meters.counter("pushapi.control.records", "result", "malformed").increment());
  }

  @Bean(destroyMethod = "")
  StatusListener statusListener(PushApiProperties p, MessageStatus status, MeterRegistry meters) {
    KafkaConsumer<String, byte[]> c = new KafkaConsumer<>(Map.of(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, p.kafka().bootstrap(),
        ConsumerConfig.GROUP_ID_CONFIG, p.kafka().statusGroup(),
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
        ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName()),
        new StringDeserializer(), new ByteArrayDeserializer());
    return new StatusListener(c, p.cell(), status,
        (topic, result) -> meters.counter("pushapi.status.updates", "topic", topic, "result", result).increment());
  }

  @Bean
  FilterRegistrationBean<PortRoutingFilter> portRoutingFilter(AdminConnector admin) {
    FilterRegistrationBean<PortRoutingFilter> b = new FilterRegistrationBean<>(new PortRoutingFilter(admin));
    b.setOrder(1);
    return b;
  }

  @Bean
  FilterRegistrationBean<BodyLimitFilter> bodyLimitFilter(PushApiProperties p) {
    FilterRegistrationBean<BodyLimitFilter> b = new FilterRegistrationBean<>(
        new BodyLimitFilter(p.http().maxBody().toBytes(), p.http().maxBatchBody().toBytes()));
    b.setOrder(2);
    return b;
  }

  @Bean
  FilterRegistrationBean<ApiAuthFilter> apiAuthFilter(ApiTokenVerifier verifier) {
    FilterRegistrationBean<ApiAuthFilter> b = new FilterRegistrationBean<>(new ApiAuthFilter(verifier));
    b.setOrder(3);
    return b;
  }

  /** Starts the background parts after the context; stops them in reverse order (push-api spec §5 SIGTERM). */
  @Bean
  SmartLifecycle pushApiBackground(PushApiProperties p, RedisState redis, RedisSeqEpochs epochs,
      SeqEpochWatcher watcher, ControlState control, ControlListener controlListener, StatusListener statusListener,
      ApiTokenVerifier verifier, KafkaRecordProducer producer, MeterRegistry meters) {
    return new SmartLifecycle() {
      private volatile boolean running;
      private ScheduledExecutorService timers;

      @Override
      public void start() {
        timers = Executors.newScheduledThreadPool(1, Thread.ofPlatform().daemon().name("push-api-timer").factory());
        epochs.refreshNow();
        long every = p.seqepochs().refresh().toMillis();
        timers.scheduleWithFixedDelay(epochs::refreshNow, every, every, TimeUnit.MILLISECONDS);
        timers.scheduleWithFixedDelay(control::prune, 60, 60, TimeUnit.SECONDS);
        timers.scheduleWithFixedDelay(() -> verifier.refresh(false), 0, 300, TimeUnit.SECONDS);
        meters.gauge("pushapi.revocations.active", control, ControlState::revocations);
        meters.gauge("pushapi.seqepoch.cache_age_seconds", epochs,
            e -> (System.currentTimeMillis() - e.refreshedAtMs()) / 1000.0);
        watcher.start(p.seqepochs().refresh());
        controlListener.start();
        statusListener.start();
        running = true;
      }

      @Override
      public void stop() {
        running = false;
        statusListener.close();
        controlListener.close();
        producer.close();
        watcher.close();
        timers.shutdownNow();
        redis.close();
      }

      @Override
      public boolean isRunning() {
        return running;
      }

      @Override
      public int getPhase() {
        return Integer.MAX_VALUE - 1000;   // stops after the web server finished its graceful shutdown
      }
    };
  }
}
