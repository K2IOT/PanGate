package com.pangate.gateway.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.pangate.gateway.loop.LoopRegistry;
import com.pangate.gateway.server.TickScheduler;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class EventLoopLagProbeTest {

  // Plain threads named "test-io": not gw-io-*, so BlockHound allows the deliberate sleep below.
  private final EventLoopGroup group =
      new MultiThreadIoEventLoopGroup(1, (java.util.concurrent.ThreadFactory) r -> new Thread(r, "test-io"),
          NioIoHandler.newFactory());
  private final TickScheduler tick = new TickScheduler();

  @AfterEach
  void stop() {
    tick.shutdown();
    group.shutdownGracefully(0, 1, java.util.concurrent.TimeUnit.SECONDS);
  }

  @Test
  void lagProbeReportsDelayOfBlockedLoop() {
    LoopRegistry loops = LoopRegistry.create(group, 128, () -> 0L);
    EventLoopLagProbe probe = new EventLoopLagProbe(loops, tick, System::nanoTime);
    probe.start();

    await().atMost(Duration.ofSeconds(10)).until(() -> probe.samples(0) >= 3);
    // Idle loop: well under the blocked level below, with room for a GC pause or CPU steal on a shared runner.
    assertThat(probe.p99Seconds(0)).isLessThan(0.2);

    // 600 ms: probes queued during the block see 500+ ms lag, so p99 (2nd largest of 100) stays >= 0.25 s.
    group.next().execute(() -> sleep(600));

    await().atMost(Duration.ofSeconds(10)).until(() -> probe.p99Seconds(0) >= 0.25);
    assertThat(probe.maxP99Seconds()).isGreaterThanOrEqualTo(0.25);
  }

  @Test
  void registersOneGaugePerLoop() {
    LoopRegistry loops = LoopRegistry.create(group, 128, () -> 0L);
    SimpleMeterRegistry meters = new SimpleMeterRegistry();

    new EventLoopLagProbe(loops, tick, System::nanoTime).bindTo(meters);

    assertThat(meters.get("gw.eventloop.lag").tag("loop", "0").gauge()).isNotNull();
  }

  private static void sleep(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
