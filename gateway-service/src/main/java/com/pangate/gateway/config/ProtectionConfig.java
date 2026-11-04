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
  com.pangate.gateway.shed.ShedSampler shedSampler(GatewayProperties p, OutboundBudget budget, LoopRegistry loops,
      com.pangate.gateway.metrics.EventLoopLagProbe lag, com.pangate.gateway.lifecycle.KafkaClients kafka,
      com.pangate.gateway.server.NodeState node, TickScheduler tick, GatewayMetrics metrics) {
    com.sun.management.OperatingSystemMXBean os = (com.sun.management.OperatingSystemMXBean)
        java.lang.management.ManagementFactory.getOperatingSystemMXBean();
    long maxDirect = PlatformDependent.maxDirectMemory();
    com.pangate.gateway.shed.CpuWindow cpu = new com.pangate.gateway.shed.CpuWindow(100);
    java.util.function.DoubleSupplier cpuSignal = p.shed().cpu() ? () -> {
      cpu.record(os.getProcessCpuLoad());
      return cpu.average();
    } : () -> 0;
    com.pangate.gateway.shed.ShedSampler sampler = new com.pangate.gateway.shed.ShedSampler(
        new com.pangate.gateway.shed.LoadShedLadder(() -> System.nanoTime() / 1_000_000), budget::ratio,
        lag::maxP99Seconds, cpuSignal, kafka::maxBufferRatio,
        () -> PlatformDependent.usedDirectMemory() / (double) maxDirect, loops,
        new com.pangate.gateway.shed.HandshakeTrend(), node, new com.pangate.gateway.shed.ShedEnforcer(loops, budget),
        () -> System.nanoTime() / 1_000_000, metrics);
    tick.scheduleAtFixedRate(sampler::sample, Duration.ofMillis(100));
    return sampler;
  }

  @Bean
  com.pangate.gateway.lifecycle.DrainCoordinator drainCoordinator(GatewayProperties p, LoopRegistry loops,
      com.pangate.gateway.server.NodeState node, com.pangate.gateway.registry.NodeIdentity identity,
      TickScheduler tick, GatewayMetrics metrics) {
    return new com.pangate.gateway.lifecycle.DrainCoordinator(p.drain(), loops, node, identity::markDraining,
        tick.executor(), metrics, bound -> java.util.concurrent.ThreadLocalRandom.current().nextLong(bound + 1));
  }

  /** Kernel counters on Linux, refreshed every 10 s on the offload executor (file reads stay off gw-tick). */
  @Bean
  java.util.Optional<com.pangate.gateway.metrics.KernelStatsReader> kernelStats(TickScheduler tick,
      java.util.concurrent.ExecutorService offload, io.micrometer.core.instrument.MeterRegistry registry) {
    java.util.Optional<com.pangate.gateway.metrics.KernelStatsReader> reader =
        com.pangate.gateway.metrics.KernelStatsReader.forThisHost();
    reader.ifPresent(r -> {
      r.refresh();
      r.bindTo(registry);
      tick.scheduleAtFixedRate(() -> offload.execute(r::refresh), Duration.ofSeconds(10));
    });
    return reader;
  }

  /** One recycler per loop (its random generator is loop-confined). */
  @Bean
  java.util.List<com.pangate.gateway.lifecycle.Recycler> recyclers(GatewayProperties p, LoopRegistry loops,
      com.pangate.gateway.server.NodeState node, com.pangate.gateway.control.TenantLimitsStore tenants,
      GatewayMetrics metrics) {
    java.util.List<com.pangate.gateway.lifecycle.Recycler> all = new java.util.ArrayList<>();
    for (com.pangate.gateway.loop.LoopContext c : loops.all()) {
      com.pangate.gateway.lifecycle.Recycler r = new com.pangate.gateway.lifecycle.Recycler(p.recycling(), node,
          tenants, new java.util.SplittableRandom(), metrics);
      c.recycler(r);
      all.add(r);
    }
    return all;
  }

  /** Kill switch admission.ratePerNode (fleet / cell), applied within a second (spec §7.8). */
  @Bean
  java.util.concurrent.ScheduledFuture<?> admissionKillSwitch(com.pangate.gateway.shed.AdmissionController admission,
      com.pangate.gateway.control.TenantLimitsStore tenants, TickScheduler tick) {
    return tick.scheduleAtFixedRate(() -> admission.applyKillSwitch(tenants.policy(null).admissionRatePerNode()),
        Duration.ofSeconds(1));
  }
}
