package com.pangate.gateway.registry;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.gateway.protocol.CloseCode;
import com.pangate.gateway.session.Session;
import com.pangate.gateway.test.LoopFixture;
import io.netty.channel.embedded.EmbeddedChannel;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RegistryResyncTest {

  private final LoopFixture f = new LoopFixture();
  private final List<String> resynced = new ArrayList<>();
  private final RegistryResync resync = new RegistryResync(f.loops, s -> resynced.add(s.connId()), 300);
  private final List<Session> sessions = new ArrayList<>();

  @BeforeEach
  void nineHundredSessions() {
    for (int i = 0; i < 900; i++) {
      Session s = f.tracker.admit(new EmbeddedChannel(), f.loop, InetAddress.getLoopbackAddress());
      s.activate(new Session.Identity("t1", "t1:u" + i, "d1", "ios", "1.0", Set.of()), "C" + i, 0L, f.wallNow(),
          f.wallNow() + 3_600, 0, List.of());
      sessions.add(s);
    }
  }

  private void elapse(long millis) {
    for (long t = 0; t < millis; t += 100) {
      f.channel.advanceTimeBy(100, TimeUnit.MILLISECONDS);
      f.channel.runScheduledPendingTasks();
      f.channel.runPendingTasks();
    }
  }

  @Test
  void resyncsActiveSessionsAt300PerLoopPerSecond() {
    resync.resync();
    f.channel.runPendingTasks();
    assertThat(resync.pending()).isEqualTo(900);

    elapse(1_000);
    assertThat(resynced).hasSize(300);

    elapse(2_000);
    assertThat(resynced).hasSize(900).doesNotHaveDuplicates();
    assertThat(resync.pending()).isZero();
  }

  @Test
  void skipsSessionsClosedMeanwhile() {
    resync.resync();
    f.channel.runPendingTasks();
    sessions.get(0).close(CloseCode.NORMAL, "bye");
    sessions.get(899).close(CloseCode.NORMAL, "bye");

    elapse(3_100);

    assertThat(resynced).hasSize(898).doesNotContain("C0", "C899");
  }

  @Test
  void newResyncSupersedesRunningOne() {
    resync.resync();
    f.channel.runPendingTasks();
    elapse(1_000);

    resync.resync();
    f.channel.runPendingTasks();
    elapse(3_100);

    assertThat(resynced).hasSize(300 + 900);
    assertThat(resync.pending()).isZero();
  }
}
