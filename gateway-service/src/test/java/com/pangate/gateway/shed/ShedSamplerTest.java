package com.pangate.gateway.shed;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.gateway.test.LoopFixture;
import io.netty.channel.embedded.EmbeddedChannel;
import java.net.InetAddress;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class ShedSamplerTest {

  private final LoopFixture f = new LoopFixture();
  private final AtomicLong ms = new AtomicLong(10_000);
  private volatile double budget;
  private volatile double direct;
  private final AtomicInteger relieved = new AtomicInteger();
  private final ShedSampler sampler = new ShedSampler(new LoadShedLadder(ms::get), () -> budget, () -> 0, () -> 0,
      () -> 0, () -> direct, f.loops, new HandshakeTrend(), f.node, relieved::incrementAndGet, ms::get, f.metrics);

  @Test
  void levelIsPublishedToNodeStateAndGauge() {
    budget = .95;
    sampler.sample();
    assertThat(f.node.shedLevel()).isEqualTo(4);
    assertThat(f.meters.get("gw.shed.level").gauge().value()).isEqualTo(4);
    assertThat(relieved.get()).isZero();
  }

  @Test
  void level5RunsTheEnforcer() {
    direct = .9;
    sampler.sample();
    assertThat(f.node.shedLevel()).isEqualTo(5);
    assertThat(relieved.get()).isEqualTo(1);
  }

  @Test
  void handshakingCountFeedsTheTrend() {
    for (int sec = 0; sec < 6; sec++) {
      f.tracker.admit(new EmbeddedChannel(), f.loop, InetAddress.getLoopbackAddress());  // never HELLOs
      for (int tick = 0; tick < 10; tick++) {
        sampler.sample();
        ms.addAndGet(100);
      }
    }
    assertThat(f.loop.handshaking()).isEqualTo(6);
    assertThat(f.node.shedLevel()).isEqualTo(3);
  }
}
