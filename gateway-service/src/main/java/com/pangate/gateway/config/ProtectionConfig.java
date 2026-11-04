package com.pangate.gateway.config;

import com.pangate.gateway.loop.LoopRegistry;
import com.pangate.gateway.metrics.GatewayMetrics;
import com.pangate.gateway.server.TickScheduler;
import com.pangate.gateway.shed.OutboundBudget;
import io.netty.util.internal.PlatformDependent;
import java.time.Duration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Node protection and lifecycle (M3a): budget, shed ladder, slow consumers, recycling, drain, warm-up (spec §7). */
@Configuration(proxyBeanMethods = false)
public class ProtectionConfig {

  @Bean
  OutboundBudget outboundBudget(GatewayProperties p, LoopRegistry loops, TickScheduler tick, GatewayMetrics metrics) {
    OutboundBudget budget = new OutboundBudget(p.outboundBudget().toBytes(), PlatformDependent.maxDirectMemory(),
        loops, metrics);
    tick.scheduleAtFixedRate(budget::sample, Duration.ofMillis(100));
    return budget;
  }
}
