package com.pangate.router.config;

import com.pangate.contracts.v1.AckItem;
import com.pangate.contracts.v1.OfflineRecord;
import com.pangate.contracts.v1.PushRecord;
import com.pangate.router.consumer.RouterConsumer;
import com.pangate.router.delivery.DeliveryJob;
import com.pangate.router.delivery.KafkaOutcomeRecorder;
import com.pangate.router.delivery.OutcomeRecorder;
import com.pangate.router.links.GatewayLinks;
import com.pangate.router.nodes.KafkaLifecyclePublisher;
import com.pangate.router.nodes.LifecyclePublisher;
import com.pangate.router.nodes.Node;
import com.pangate.router.nodes.NodeDirectory;
import com.pangate.router.nodes.NodeDownWatcher;
import com.pangate.router.nodes.RedisNodeSource;
import com.pangate.router.registry.LiveConnection;
import com.pangate.router.registry.RegistryLookup;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;
import io.micrometer.core.instrument.MeterRegistry;
import java.net.InetAddress;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Wires the Spring-free core (push-router spec §1) and runs it as one lifecycle. */
@Configuration(proxyBeanMethods = false)
public class RouterConfig {

  private static final Logger log = LoggerFactory.getLogger(RouterConfig.class);
  /** Readiness drops after this long without a directory refresh (spec §4). */
  static final Duration DIRECTORY_STALE_AFTER = Duration.ofSeconds(10);
  /** SIGTERM: wait at most this long for records in flight (spec §4). */
  static final Duration STOP_WAIT = Duration.ofSeconds(10);

  static long monoMs() {
    return System.nanoTime() / 1_000_000;
  }

  @Bean
  RouterMetrics routerMetrics(MeterRegistry registry) {
    return new RouterMetrics(registry);
  }

  /** The registry Redis: standalone or cluster per {@code router.registry.topology}. */
  record Registry(RedisClusterAsyncCommands<String, String> async, AutoCloseable close) {}

  @Bean(destroyMethod = "")
  Registry registry(RouterProperties p) {
    ClientResources res = DefaultClientResources.create();
    RedisURI uri = RedisURI.create(p.registry().uri());
    if ("cluster".equals(p.registry().topology())) {
      RedisClusterClient client = RedisClusterClient.create(res, uri);
      StatefulRedisClusterConnection<String, String> c = client.connect();
      return new Registry(c.async(), () -> {
        c.close();
        client.shutdown();
        res.shutdown();
      });
    }
    RedisClient client = RedisClient.create(res, uri);
    var c = client.connect();
    StatefulConnection<String, String> conn = c;
    return new Registry(c.async(), () -> {
      conn.close();
      client.shutdown();
      res.shutdown();
    });
  }

  @Bean
  NodeDirectory nodeDirectory(Registry registry, RouterProperties p) {
    return new NodeDirectory(new RedisNodeSource(registry.async(), p.registry().timeout()), System::currentTimeMillis,
        RouterConfig::monoMs, DIRECTORY_STALE_AFTER);
  }

  @Bean(destroyMethod = "")
  Producer<String, byte[]> outcomeProducer(RouterProperties p) {
    Map<String, Object> cfg = new HashMap<>();
    cfg.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, p.kafka().bootstrap());
    cfg.put(ProducerConfig.ACKS_CONFIG, "all");
    cfg.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
    cfg.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");
    cfg.put(ProducerConfig.LINGER_MS_CONFIG, 2);
    cfg.put(ProducerConfig.CLIENT_ID_CONFIG, "push-router-outcomes");
    return new KafkaProducer<>(cfg, new StringSerializer(), new ByteArraySerializer());
  }

  @Bean(destroyMethod = "")
  GatewayLinks gatewayLinks(NodeDirectory dir, RouterProperties p, RouterMetrics metrics) {
    GatewayLinks links = new GatewayLinks(dir, n -> channel(n, p.grpc()), p.batch().max(), p.batch().linger(),
        p.maxInFlightPerNode());
    metrics.links(links::openLinks);
    return links;
  }

  static ManagedChannel channel(Node n, RouterProperties.Grpc grpc) {
    // keepalive finds a half-open connection in ~15 s (spec §3.3); the gateway permits pings every 10 s
    NettyChannelBuilder b = NettyChannelBuilder.forTarget(n.address()).keepAliveTime(10, TimeUnit.SECONDS)
        .keepAliveTimeout(5, TimeUnit.SECONDS);
    if (grpc.plaintext()) {
      b.usePlaintext();
    } else {
      try {
        b.sslContext(GrpcSslContexts.forClient().keyManager(grpc.tls().cert().toFile(), grpc.tls().key().toFile())
            .trustManager(grpc.tls().trust().toFile()).build());
      } catch (SSLException e) {
        throw new IllegalStateException("router gRPC TLS material unusable", e);
      }
    }
    return b.build();
  }

  @Bean
  RouterConsumer routerConsumer(Registry registry, NodeDirectory dir, GatewayLinks links,
      Producer<String, byte[]> producer, RouterProperties p, RouterMetrics metrics) {
    RegistryLookup lookup = new RegistryLookup(principal -> {
      long t0 = System.nanoTime();
      return registry.async().hgetall("reg:{" + principal + "}")
          .whenComplete((v, e) -> metrics.registryLatency(System.nanoTime() - t0));
    }, dir, p.registry().cache(), p.registry().timeout(), RouterConfig::monoMs);
    OutcomeRecorder kafka = new KafkaOutcomeRecorder(producer, p.cell(), System::currentTimeMillis);
    OutcomeRecorder recorder = new OutcomeRecorder() {
      @Override
      public void offline(PushRecord r, OfflineRecord.Reason reason, int attempts) {
        kafka.offline(r, reason, attempts);
        metrics.offline(reason);
      }

      @Override
      public void delivered(PushRecord r, LiveConnection c, AckItem.Status s) {
        kafka.delivered(r, c, s);
      }
    };
    DeliveryJob job = new DeliveryJob(lookup, links, () -> dir.requireFresh().values(), recorder, System::currentTimeMillis,
        p.ackTimeout(), p.retries(), d -> Thread.sleep(d));
    return new RouterConsumer(job::run, metrics, p.cell() + ".push.requests", System::currentTimeMillis);
  }

  /** Bean name {@code routerHealthIndicator} → health contributor {@code router} (readiness group). */
  @Bean
  RouterHealth routerHealthIndicator(NodeDirectory dir, RouterConsumer consumer) {
    return new RouterHealth(dir, consumer);
  }

  @Bean
  SmartLifecycle routerLifecycle(Registry registry, NodeDirectory dir, GatewayLinks links,
      Producer<String, byte[]> producer, RouterConsumer consumer, RouterProperties p, RouterMetrics metrics) {
    return new SmartLifecycle() {
      private volatile boolean running;
      private ScheduledExecutorService refresher;

      @Override
      public void start() {
        LifecyclePublisher kafka = new KafkaLifecyclePublisher(producer, p.cell(), System::currentTimeMillis);
        NodeDownWatcher watcher = new NodeDownWatcher((id, epoch) -> {
          metrics.nodeDown();
          kafka.nodeDown(id, epoch);
        });
        dir.onChange(watcher::accept);
        dir.refresh();
        refresher = Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().daemon().name("node-directory")
            .factory());
        long every = p.directoryRefresh().toMillis();
        refresher.scheduleWithFixedDelay(dir::refresh, every, every, TimeUnit.MILLISECONDS);
        consumer.start(new KafkaConsumer<>(consumerConfig(p), new StringDeserializer(), new ByteArrayDeserializer()),
            p.kafka().maxConcurrency());
        running = true;
        log.info("router started: cell {}, group {}", p.cell(), p.kafka().group());
      }

      @Override
      public void stop() {
        running = false;
        consumer.stop(STOP_WAIT);
        producer.close(Duration.ofSeconds(5));   // a job still running cannot write an outcome any more
        links.close();
        refresher.shutdownNow();
        try {
          registry.close().close();
        } catch (Exception e) {
          log.warn("registry client did not close cleanly: {}", e.toString());
        }
      }

      @Override
      public boolean isRunning() {
        return running;
      }
    };
  }

  static Map<String, Object> consumerConfig(RouterProperties p) {
    Map<String, Object> cfg = new HashMap<>();
    cfg.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, p.kafka().bootstrap());
    cfg.put(ConsumerConfig.GROUP_ID_CONFIG, p.kafka().group());
    cfg.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);   // Parallel Consumer commits
    cfg.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    cfg.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());
    cfg.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30_000);
    cfg.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, hostname());
    return cfg;
  }

  static String hostname() {
    String h = System.getenv("HOSTNAME");
    if (h != null && !h.isBlank()) {
      return h;
    }
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (Exception e) {
      return "push-router";
    }
  }
}
