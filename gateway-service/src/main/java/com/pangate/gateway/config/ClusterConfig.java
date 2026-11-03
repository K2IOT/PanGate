package com.pangate.gateway.config;

import com.pangate.gateway.control.CellMapView;
import com.pangate.gateway.control.ControlEventConsumer;
import com.pangate.gateway.control.ControlEventHandler;
import com.pangate.gateway.control.ControlState;
import com.pangate.gateway.control.RevocationList;
import com.pangate.gateway.control.TenantLimitsStore;
import com.pangate.gateway.lifecycle.InfraLifecycle;
import com.pangate.gateway.lifecycle.KafkaClients;
import com.pangate.gateway.lifecycle.ProcessExit;
import com.pangate.gateway.lifecycle.RedisConnections;
import com.pangate.gateway.lifecycle.TopicNames;
import com.pangate.gateway.metrics.GatewayMetrics;
import com.pangate.gateway.mgmt.ReadinessCheck;
import com.pangate.gateway.registry.NodeIdentity;
import com.pangate.gateway.registry.RegistryWriter;
import com.pangate.gateway.registry.RetryQueue;
import com.pangate.gateway.server.NodeState;
import com.pangate.gateway.server.TickScheduler;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Cluster wiring (M2): Redis, Kafka, node identity, registry, upstream and control (spec §6). */
@Configuration(proxyBeanMethods = false)
public class ClusterConfig {

  @Bean
  ProcessExit processExit() {
    return ProcessExit.system();
  }

  @Bean
  RedisConnections redisConnections(GatewayProperties p) {
    return new RedisConnections(p.redis());
  }

  @Bean
  KafkaClients kafkaClients(GatewayProperties p) {
    return new KafkaClients(p.kafka(), p.nodeId());
  }

  @Bean
  TopicNames topicNames(GatewayProperties p) {
    return new TopicNames(p.kafka().topicPrefix(), p.kafka().upstreamTypes());
  }

  @Bean
  InfraLifecycle infraLifecycle(GatewayProperties p, RedisConnections redis, KafkaClients kafka, TopicNames topics) {
    return new InfraLifecycle(p.startup().dependencyTimeout(), redis, kafka, topics, p.kafka().createTopics());
  }

  @Bean
  NodeIdentity nodeIdentity(GatewayProperties p, RedisConnections redis, NodeState node, TickScheduler tick,
      ProcessExit exit, GatewayMetrics metrics) {
    return new NodeIdentity(p.nodeId(), redis, node, tick, () -> System.nanoTime() / 1_000_000,
        System::currentTimeMillis, exit, metrics);
  }

  @Bean
  ReadinessCheck leaseAliveCheck(NodeState node) {
    return () -> node.leaseAlive() ? Optional.empty() : Optional.of("node lease lost");
  }

  @Bean
  RevocationList revocationList(TickScheduler tick, GatewayMetrics metrics) {
    RevocationList list = new RevocationList();
    metrics.gauge("gw.revocations", list, RevocationList::size);
    tick.scheduleAtFixedRate(() -> list.sweep(System.currentTimeMillis()), Duration.ofSeconds(60));
    return list;
  }

  @Bean
  CellMapView cellMapView(GatewayProperties p) {
    return new CellMapView(p.cell());
  }

  @Bean
  TenantLimitsStore tenantLimitsStore(GatewayProperties p, LimitsHolder limits) {
    return new TenantLimitsStore(p.cell(), limits::current);
  }

  @Bean
  ControlState controlState(RevocationList revocations, CellMapView cells, TenantLimitsStore tenants) {
    return new ControlState(revocations, cells, tenants, System::currentTimeMillis);
  }

  /** ControlState first: enforcement handlers (registered as {@code ControlEventHandler} beans) see updated stores. */
  @Bean
  ControlEventConsumer controlEventConsumer(GatewayProperties p, KafkaClients kafka, NodeState node,
      ControlState state, ObjectProvider<ControlEventHandler> enforcers, GatewayMetrics metrics, ProcessExit exit,
      InfraLifecycle infra) {
    List<ControlEventHandler> handlers = new ArrayList<>();
    handlers.add(state);
    enforcers.orderedStream().filter(h -> h != state).forEach(handlers::add);
    ControlEventConsumer consumer = new ControlEventConsumer(kafka::newControlConsumer, TopicNames.CONTROL, node,
        handlers, metrics, exit, Duration.ofMinutes(5), () -> System.nanoTime() / 1_000_000);
    infra.onStarted(consumer::start, consumer::stop);
    return consumer;
  }

  @Bean
  ReadinessCheck controlCaughtUpCheck(NodeState node) {
    return () -> node.controlCaughtUp() ? Optional.empty() : Optional.of("gw.control not caught up");
  }

  @Bean
  RetryQueue registryRetryQueue(TickScheduler tick, GatewayMetrics metrics) {
    return new RetryQueue((r, d) -> tick.executor().schedule(r, d, java.util.concurrent.TimeUnit.MILLISECONDS),
        10_000, metrics);
  }

  @Bean
  RegistryWriter registryWriter(RedisConnections redis, NodeState node, RetryQueue retry, GatewayMetrics metrics) {
    return new RegistryWriter(redis, node, retry, metrics);
  }
}
