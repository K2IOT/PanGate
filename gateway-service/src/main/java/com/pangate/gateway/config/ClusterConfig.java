package com.pangate.gateway.config;

import com.pangate.gateway.lifecycle.InfraLifecycle;
import com.pangate.gateway.lifecycle.KafkaClients;
import com.pangate.gateway.lifecycle.ProcessExit;
import com.pangate.gateway.lifecycle.RedisConnections;
import com.pangate.gateway.lifecycle.TopicNames;
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
}
