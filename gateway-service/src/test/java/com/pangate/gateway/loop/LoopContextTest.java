package com.pangate.gateway.loop;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.pangate.gateway.config.GatewayProperties;
import com.pangate.gateway.server.IoGroups;
import com.pangate.gateway.server.Transport;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class LoopContextTest {

  private final IoGroups io = new IoGroups(new GatewayProperties.Io(4, Transport.NIO));
  private final AtomicLong clock = new AtomicLong(100);

  @AfterEach
  void stop() {
    io.shutdown(Duration.ZERO, Duration.ofSeconds(2)).syncUninterruptibly();
  }

  static final class Probe extends WheelNode implements Deadlined {
    final AtomicReference<String> firedOn = new AtomicReference<>();

    @Override
    public void onDeadline(long nowSec) {
      firedOn.set(Thread.currentThread().getName());
    }
  }

  @Test
  void registryIndexesEveryLoop() {
    LoopRegistry loops = LoopRegistry.create(io.group(), 128, clock::get);

    assertThat(loops.size()).isEqualTo(4);
    for (int i = 0; i < 4; i++) {
      assertThat(loops.of(loops.get(i).eventLoop()).index()).isEqualTo(i);
    }
    assertThat(loops.all()).hasSize(4);
    assertThat(loops.totalActive()).isZero();
  }

  @Test
  void sessionKeysEmbedLoopIndexAndIncrease() {
    LoopContext ctx = LoopRegistry.create(io.group(), 128, clock::get).get(3);

    long a = ctx.nextSessionKey();
    long b = ctx.nextSessionKey();

    assertThat(a >>> 40).isEqualTo(3);
    assertThat(b).isGreaterThan(a);
  }

  @Test
  void activeCountTracksIncrementsAndDecrements() {
    LoopContext ctx = LoopRegistry.create(io.group(), 128, clock::get).get(0);

    ctx.incrementActive();
    ctx.incrementActive();
    ctx.decrementActive();

    assertThat(ctx.activeCount()).isEqualTo(1);
  }

  @Test
  void tickerAdvancesWheelOnLoop() {
    LoopRegistry loops = LoopRegistry.create(io.group(), 128, clock::get);
    LoopContext ctx = loops.get(1);
    Probe probe = new Probe();
    ctx.eventLoop().execute(() -> ctx.wheel().schedule(probe, 101));

    ctx.startTicker();
    clock.set(101);

    await().atMost(Duration.ofSeconds(3)).until(() -> probe.firedOn.get() != null);
    assertThat(probe.firedOn.get()).startsWith("gw-io-");
  }
}
