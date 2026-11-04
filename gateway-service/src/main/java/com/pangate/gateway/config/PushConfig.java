package com.pangate.gateway.config;

import com.pangate.gateway.control.TenantLimitsStore;
import com.pangate.gateway.downstream.BroadcastPacer;
import com.pangate.gateway.downstream.ClientTopics;
import com.pangate.gateway.downstream.ControlPushHandler;
import com.pangate.gateway.downstream.GrpcServerLifecycle;
import com.pangate.gateway.downstream.PushDelivery;
import com.pangate.gateway.downstream.PushDispatcher;
import com.pangate.gateway.downstream.PushStreamService;
import com.pangate.gateway.loop.LoopContext;
import com.pangate.gateway.loop.LoopRegistry;
import com.pangate.gateway.metrics.GatewayMetrics;
import com.pangate.gateway.server.NodeState;
import com.pangate.gateway.server.TickScheduler;
import com.pangate.gateway.session.PrincipalIndex;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Downstream wiring (M2b): delivery, topics, broadcast, control push, gRPC PushStream and KICK (spec §6.3–§6.6). */
@Configuration(proxyBeanMethods = false)
public class PushConfig {

  @Bean
  PushDelivery pushDelivery(TenantLimitsStore tenants, LimitsHolder limits, GatewayMetrics metrics) {
    return new PushDelivery(tenants, limits, metrics, System::currentTimeMillis);
  }

  @Bean
  ClientTopics clientTopics(TenantLimitsStore tenants, GatewayMetrics metrics) {
    return new ClientTopics(tenants, metrics);
  }

  @Bean
  ControlPushHandler controlPushHandler(PrincipalIndex principals, TenantLimitsStore tenants, GatewayMetrics metrics) {
    return new ControlPushHandler(principals, tenants, metrics);
  }

  /**
   * gw-grpc-exec: gRPC call executor, batch dispatch and ack flushing (spec §3.2). Wrapped in its own type so it is
   * never injected where an {@code ExecutorService} (the offload executor) is expected.
   */
  public record GrpcExecutor(ScheduledExecutorService service) implements AutoCloseable {

    static GrpcExecutor create() {
      AtomicInteger n = new AtomicInteger();
      return new GrpcExecutor(Executors.newScheduledThreadPool(2, r -> {
        Thread t = new Thread(r, "gw-grpc-exec-" + n.incrementAndGet());
        t.setDaemon(true);
        return t;
      }));
    }

    @Override
    public void close() {
      service.shutdownNow();
    }
  }

  @Bean
  GrpcExecutor grpcExecutor() {
    return GrpcExecutor.create();
  }

  /** Every loop gets its broadcast pacer before the first batch can arrive. */
  @Bean
  PushDispatcher pushDispatcher(LoopRegistry loops, PrincipalIndex principals, NodeState node, PushDelivery delivery,
      ControlPushHandler control, TenantLimitsStore tenants, LimitsHolder limits, GatewayMetrics metrics) {
    for (LoopContext loop : loops.all()) {
      loop.pacer(new BroadcastPacer(loop, node, tenants, limits, metrics));
    }
    return new PushDispatcher(loops, principals, node, delivery, control, metrics);
  }

  @Bean
  PushStreamService pushStreamService(PushDispatcher dispatcher, NodeState node, GrpcExecutor grpcExecutor,
      TickScheduler tick, GatewayMetrics metrics) {
    PushStreamService svc = new PushStreamService(dispatcher::dispatch, node, grpcExecutor.service(), metrics);
    tick.scheduleAtFixedRate(svc::resumePaused, Duration.ofMillis(100));
    return svc;
  }

  /** KICK through the Push API, or only a WARN when it is disabled (local: one node, spec §6.6). */
  @Bean  // PushApiKickClient is AutoCloseable: Spring closes its channel on shutdown
  com.pangate.gateway.registry.KickClient kickClient(GatewayProperties p, TickScheduler tick, GatewayMetrics metrics) {
    return p.pushApi().enabled()
        ? com.pangate.gateway.registry.PushApiKickClient.create(p.pushApi(), p.grpc().tls(), tick.executor(), metrics)
        : com.pangate.gateway.registry.KickClient.disabled();
  }

  @Bean
  com.pangate.gateway.registry.SingleSessionKicker singleSessionKicker(PrincipalIndex principals, LoopRegistry loops,
      NodeState node, ControlPushHandler control, com.pangate.gateway.registry.KickClient kickClient) {
    return new com.pangate.gateway.registry.SingleSessionKicker(principals, loops, node, control, kickClient);
  }

  @Bean
  GrpcServerLifecycle grpcServerLifecycle(GatewayProperties p, PushStreamService svc,
      GrpcExecutor grpcExecutor) {
    return new GrpcServerLifecycle(p.grpc(), svc, grpcExecutor.service());
  }
}
