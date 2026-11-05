package com.pangate.gateway.trace;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.gateway.session.Session;
import com.pangate.gateway.test.LoopFixture;
import io.netty.channel.embedded.EmbeddedChannel;
import java.net.InetAddress;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class TraceTargetsTest {

  private static final long T0 = 1_762_000_000_000L;
  private static final long MIN = 60_000L;

  private final LoopFixture f = new LoopFixture();
  private final AtomicLong now = new AtomicLong(T0);
  private final TraceTargets targets = new TraceTargets(f.principals, f.loops, PrincipalHasher.random(), now::get);

  @Test
  void ttlIsCappedAt15Minutes() {
    targets.put("t1:u1", T0, T0 + 60 * MIN);
    assertThat(targets.traced("t1:u1", T0 + 14 * MIN)).isTrue();
    assertThat(targets.traced("t1:u1", T0 + 15 * MIN + 1)).isFalse();
  }

  @Test
  void missingExpiryMeans15Minutes() {
    targets.put("t1:u1", T0, 0);
    assertThat(targets.traced("t1:u1", T0 + 15 * MIN - 1)).isTrue();
    assertThat(targets.traced("t1:u1", T0 + 15 * MIN + 1)).isFalse();
  }

  @Test
  void earlierExpiryWins() {
    targets.put("t1:u1", T0, T0 + MIN);
    assertThat(targets.traced("t1:u1", T0 + 2 * MIN)).isFalse();
  }

  @Test
  void tombstoneRemoves() {
    targets.put("t1:u1", T0, 0);
    targets.remove("t1:u1");
    assertThat(targets.traced("t1:u1", T0)).isFalse();
  }

  @Test
  void sweepReturnsExpired() {
    targets.put("t1:u1", T0, T0 + MIN);
    targets.put("t1:u2", T0, 0);
    assertThat(targets.sweep(T0 + 2 * MIN)).containsExactly("t1:u1");
    assertThat(targets.traced("t1:u2", T0 + 2 * MIN)).isTrue();
  }

  private Session active(String principal) {
    EmbeddedChannel ch = new EmbeddedChannel();
    Session s = f.tracker.admit(ch, f.loop, InetAddress.getLoopbackAddress());
    s.activate(new Session.Identity("t1", principal, "d1", "ios", "1.0", Set.of()), f.tracker.newConnId(s), 0L,
        f.wallNow(), f.wallNow() + 3600, 1, List.of());
    f.tracker.onActivated(s);
    return s;
  }

  @Test
  void tracedFlagFollowsLiveSessions() {
    f.listeners.add(targets);
    Session s = active("t1:u1");
    Session other = active("t1:u2");
    assertThat(s.traced()).isFalse();

    targets.onTrace("t1:u1", now.get(), 0);
    f.channel.runPendingTasks();
    assertThat(s.traced()).isTrue();
    assertThat(other.traced()).isFalse();

    targets.onTraceRemoved("t1:u1");
    f.channel.runPendingTasks();
    assertThat(s.traced()).isFalse();
  }

  @Test
  void activationOfATracedPrincipalSetsTheFlag() {
    f.listeners.add(targets);
    targets.onTrace("t1:u1", now.get(), 0);
    assertThat(active("t1:u1").traced()).isTrue();
  }

  @Test
  void sweepExpiredClearsFlags() {
    f.listeners.add(targets);
    targets.onTrace("t1:u1", now.get(), now.get() + MIN);
    Session s = active("t1:u1");
    assertThat(s.traced()).isTrue();

    now.addAndGet(2 * MIN);
    targets.sweepExpired();
    f.channel.runPendingTasks();
    assertThat(s.traced()).isFalse();
  }
}
