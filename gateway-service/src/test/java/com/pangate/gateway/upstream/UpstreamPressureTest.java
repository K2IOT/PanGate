package com.pangate.gateway.upstream;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.gateway.session.Session;
import com.pangate.gateway.test.LoopFixture;
import io.netty.channel.embedded.EmbeddedChannel;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class UpstreamPressureTest {

  private final LoopFixture f = new LoopFixture();
  private final AtomicReference<Double> ratio = new AtomicReference<>(0.0);
  private final UpstreamPressure pressure = new UpstreamPressure(ratio::get, f.loops, f.metrics);
  private final List<Session> sessions = new ArrayList<>();

  @BeforeEach
  void twoHundredSessions() {
    for (int i = 0; i < 200; i++) {
      EmbeddedChannel ch = new EmbeddedChannel();
      Session s = f.tracker.admit(ch, f.loop, InetAddress.getLoopbackAddress());
      s.activate(new Session.Identity("t1", "t1:u" + i, "d1", "ios", "1.0", Set.of()), "C" + i, 0L, f.wallNow(),
          f.wallNow() + 3_600, 0, List.of());
      s.addUpstreamBytes(i * 10, f.now());
      sessions.add(s);
    }
  }

  private void sample(double r) {
    ratio.set(r);
    pressure.sample();
    f.channel.runPendingTasks();
  }

  private List<Integer> pausedIndexes() {
    List<Integer> out = new ArrayList<>();
    for (int i = 0; i < sessions.size(); i++) {
      if (!sessions.get(i).channel().config().isAutoRead()) {
        out.add(i);
      }
    }
    return out;
  }

  @Test
  void pausesTopOnePercentNoisiestAbove90() {
    sample(0.95);

    assertThat(pausedIndexes()).containsExactlyInAnyOrder(198, 199);
    assertThat(sessions.get(199).upstreamPaused()).isTrue();
    assertThat(pressure.paused()).isTrue();
  }

  @Test
  void staysPausedBetween50And90() {
    sample(0.95);
    sessions.get(10).addUpstreamBytes(1_000_000, f.now());

    sample(0.7);
    sample(0.95);

    assertThat(pausedIndexes()).containsExactlyInAnyOrder(198, 199);
  }

  @Test
  void resumesAllBelow50() {
    sample(0.95);

    sample(0.4);

    assertThat(pausedIndexes()).isEmpty();
    assertThat(sessions.get(199).upstreamPaused()).isFalse();
    assertThat(pressure.paused()).isFalse();
  }

  @Test
  void idleSessionsAreNotPaused() {
    LoopFixture g = new LoopFixture();
    Session quiet = g.tracker.admit(g.channel, g.loop, InetAddress.getLoopbackAddress());
    quiet.activate(new Session.Identity("t1", "t1:q", "d1", "ios", "1.0", Set.of()), "Q", 0L, g.wallNow(),
        g.wallNow() + 3_600, 0, List.of());
    UpstreamPressure p = new UpstreamPressure(() -> 0.99, g.loops, g.metrics);

    p.sample();
    g.channel.runPendingTasks();

    assertThat(g.channel.config().isAutoRead()).isTrue();
  }

  @Test
  void staleWindowCountsAsZero() {
    Session s = sessions.get(5);
    s.addUpstreamBytes(100, f.now());

    assertThat(s.upstreamBytes10s(f.now() + 9)).isEqualTo(150);
    assertThat(s.upstreamBytes10s(f.now() + 11)).isZero();
    s.addUpstreamBytes(7, f.now() + 11);
    assertThat(s.upstreamBytes10s(f.now() + 11)).isEqualTo(7);
  }
}
