package com.pangate.gateway.lifecycle;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

/**
 * Connects the gateway's dependencies before anything that needs them starts (spec §3.7, phase MAX − 1900): Redis,
 * Kafka (cluster reachable, topics present or created, producer metadata warm). Each wait is bounded by
 * {@code gateway.startup.dependency-timeout}; past it a {@code DependencyUnavailableException} stops the process with
 * a one-line diagnostic (R4). The management port is already up (Tomcat, MAX − 2048).
 */
public final class InfraLifecycle implements SmartLifecycle {

  private static final Logger log = LoggerFactory.getLogger(InfraLifecycle.class);
  public static final int PHASE = Integer.MAX_VALUE - 1900;

  private final Duration timeout;
  private final RedisConnections redis;
  private final KafkaClients kafka;
  private final TopicNames topics;
  private final boolean createTopics;
  private final java.util.List<Runnable[]> hooks = new java.util.concurrent.CopyOnWriteArrayList<>();
  private volatile boolean running;

  public InfraLifecycle(Duration timeout, RedisConnections redis, KafkaClients kafka, TopicNames topics,
      boolean createTopics) {
    this.timeout = timeout;
    this.redis = redis;
    this.kafka = kafka;
    this.topics = topics;
    this.createTopics = createTopics;
  }

  /**
   * Work that needs the connected clients (gw.control consumer, seq epochs): {@code start} runs at the end of
   * {@link #start()} in registration order, {@code stop} at the beginning of {@link #stop()} in reverse order.
   */
  public void onStarted(Runnable start, Runnable stop) {
    hooks.add(new Runnable[] {start, stop});
  }

  @Override
  public void start() {
    Retry.untilDeadline("Redis", redis.registryUri(), "GATEWAY_REDIS_REGISTRY_URI", timeout,
        System::currentTimeMillis, Retry.Sleeper.SYSTEM, () -> {
          redis.connect();
          redis.registry().ping().get(5, TimeUnit.SECONDS);
          redis.state().ping().get(5, TimeUnit.SECONDS);
          return null;
        });
    Retry.untilDeadline("Kafka", kafka.bootstrap(), "GATEWAY_KAFKA_BOOTSTRAP", timeout, System::currentTimeMillis,
        Retry.Sleeper.SYSTEM, () -> kafka.admin().describeCluster().nodes().get(5, TimeUnit.SECONDS));
    TopicProvisioner.ensure(kafka.admin(), topics, createTopics, Duration.ofSeconds(30));
    kafka.startProducers();
    // max.block.ms=0 fails a send whose topic metadata is not cached yet: warm it here, off the event loops.
    for (String type : topics.upstreamTypes()) {
      warm(TopicNames.isReliable(type) ? kafka.reliable() : kafka.telemetry(), topics.upstream(type));
    }
    warm(kafka.reliable(), topics.lifecycle());
    hooks.forEach(h -> h[0].run());
    running = true;
    log.info("infrastructure ready: redis {}, kafka {}", redis.registryUri(), kafka.bootstrap());
  }

  /** Each failed partitionsFor (max.block.ms=0) still queues a metadata fetch for the topic; poll until cached. */
  private void warm(org.apache.kafka.clients.producer.Producer<String, byte[]> producer, String topic) {
    Retry.untilDeadline("Kafka", kafka.bootstrap() + " (metadata of " + topic + ")", "GATEWAY_KAFKA_BOOTSTRAP",
        timeout, System::currentTimeMillis, d -> Thread.sleep(Math.min(d.toMillis(), 100)),
        () -> producer.partitionsFor(topic));
  }

  @Override
  public void stop() {
    running = false;
    for (int i = hooks.size() - 1; i >= 0; i--) {
      try {
        hooks.get(i)[1].run();
      } catch (RuntimeException e) {
        log.warn("infrastructure stop hook failed", e);
      }
    }
    kafka.close(Duration.ofSeconds(10));
    redis.close();
  }

  @Override
  public boolean isRunning() {
    return running;
  }

  @Override
  public int getPhase() {
    return PHASE;
  }

  public RedisConnections redis() {
    return redis;
  }

  public KafkaClients kafka() {
    return kafka;
  }
}
