package com.pangate.router.config;

import com.pangate.router.consumer.RouterConsumer;
import com.pangate.router.nodes.NodeDirectory;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;

/**
 * Readiness "router": the node directory refreshed within the last 10 s and the consumer is polling (push-router spec
 * §4). A router that cannot see the gateways must not take records.
 */
public final class RouterHealth implements HealthIndicator {

  private final NodeDirectory dir;
  private final RouterConsumer consumer;

  public RouterHealth(NodeDirectory dir, RouterConsumer consumer) {
    this.dir = dir;
    this.consumer = consumer;
  }

  @Override
  public Health health() {
    boolean fresh = dir.fresh();
    boolean polling = consumer.running();
    Health.Builder b = fresh && polling ? Health.up() : Health.outOfService();
    return b.withDetail("directoryFresh", fresh).withDetail("nodes", dir.live().size())
        .withDetail("consumer", polling ? "polling" : "stopped").build();
  }
}
