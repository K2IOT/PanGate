package com.pangate.gateway.config;

import com.pangate.gateway.control.TenantLimitsStore;
import com.pangate.gateway.downstream.PushDelivery;
import com.pangate.gateway.metrics.GatewayMetrics;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Downstream wiring (M2b): delivery, topics, broadcast, control push, gRPC PushStream and KICK (spec §6.3–§6.6). */
@Configuration(proxyBeanMethods = false)
public class PushConfig {

  @Bean
  PushDelivery pushDelivery(TenantLimitsStore tenants, LimitsHolder limits, GatewayMetrics metrics) {
    return new PushDelivery(tenants, limits, metrics, System::currentTimeMillis);
  }
}
