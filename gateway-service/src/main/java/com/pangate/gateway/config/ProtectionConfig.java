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

  @Bean
  com.pangate.gateway.shed.ShedSampler shedSampler(OutboundBudget budget, LoopRegistry loops,
      com.pangate.gateway.metrics.EventLoopLagProbe lag, com.pangate.gateway.lifecycle.KafkaClients kafka,
      com.pangate.gateway.server.NodeState node, TickScheduler tick, GatewayMetrics metrics) {
    com.sun.management.OperatingSystemMXBean os = (com.sun.management.OperatingSystemMXBean)
        java.lang.management.ManagementFactory.getOperatingSystemMXBean();
    long maxDirect = PlatformDependent.maxDirectMemory();
    com.pangate.gateway.shed.ShedSampler sampler = new com.pangate.gateway.shed.ShedSampler(
        new com.pangate.gateway.shed.LoadShedLadder(() -> System.nanoTime() / 1_000_000), budget::ratio,
        lag::maxP99Seconds, os::getProcessCpuLoad, kafka::maxBufferRatio,
        () -> PlatformDependent.usedDirectMemory() / (double) maxDirect, loops,
        new com.pangate.gateway.shed.HandshakeTrend(), node, new com.pangate.gateway.shed.ShedEnforcer(loops, budget),
        () -> System.nanoTime() / 1_000_000, metrics);
    tick.scheduleAtFixedRate(sampler::sample, Duration.ofMillis(100));
    return sampler;
  }

  /** Kill switch admission.ratePerNode (fleet / cell), applied within a second (spec §7.8). */
  @Bean
  java.util.concurrent.ScheduledFuture<?> admissionKillSwitch(com.pangate.gateway.shed.AdmissionController admission,
      com.pangate.gateway.control.TenantLimitsStore tenants, TickScheduler tick) {
    return tick.scheduleAtFixedRate(() -> admission.applyKillSwitch(tenants.policy(null).admissionRatePerNode()),
        Duration.ofSeconds(1));
  }
}
