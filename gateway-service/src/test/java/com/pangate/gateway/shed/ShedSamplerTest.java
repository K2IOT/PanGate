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
    f.node.warmedUp(true);
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

  @Test
  void startUpCpuAndWarmUpHandshakesDoNotRaiseTheLevelBeforeWarmedUp() {
    ShedSampler hot = new ShedSampler(new LoadShedLadder(ms::get), () -> 0, () -> 0, () -> .95, () -> 0, () -> 0,
        f.loops, new HandshakeTrend(), f.node, relieved::incrementAndGet, ms::get, f.metrics);
    for (int tick = 0; tick < 10; tick++) {
      hot.sample();
      ms.addAndGet(100);
    }
    assertThat(f.node.shedLevel()).isZero();

    f.node.warmedUp(true);
    hot.sample();
    assertThat(f.node.shedLevel()).isEqualTo(3);
  }

  @Test
  void flappingBetween4And5LogsAtMostOncePerSecond() {
    ch.qos.logback.classic.Logger log = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(ShedSampler.class);
    ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent> lines =
        new ch.qos.logback.core.read.ListAppender<>();
    lines.start();
    log.addAppender(lines);
    try {
      budget = .95;
      for (int tick = 0; tick < 20; tick++) {  // 2 s of samples, the level changes on every one
        direct = tick % 2 == 0 ? .9 : 0;
        sampler.sample();
        ms.addAndGet(100);
      }
    } finally {
      log.detachAppender(lines);
    }
    assertThat(lines.list).hasSizeBetween(1, 2);
    assertThat(lines.list.get(lines.list.size() - 1).getFormattedMessage()).contains("suppressed");
  }
}
