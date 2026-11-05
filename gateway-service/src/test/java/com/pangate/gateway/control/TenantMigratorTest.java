package com.pangate.gateway.control;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.contracts.v1.Frame;
import com.pangate.contracts.v1.FrameType;
import com.pangate.contracts.v1.Reconnect;
import com.pangate.gateway.session.Session;
import com.pangate.gateway.session.SessionState;
import com.pangate.gateway.test.LoopFixture;
import io.netty.channel.embedded.EmbeddedChannel;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TenantMigratorTest {

  private final LoopFixture f = new LoopFixture();
  private final TenantMigrator migrator = new TenantMigrator(f.loops, f.cells, () -> 1_500L);
  private final List<Session> sessions = new ArrayList<>();

  @BeforeEach
  void hundredSessionsOfT1() {
    for (int i = 0; i < 100; i++) {
      Session s = f.tracker.admit(new EmbeddedChannel(), f.loop, InetAddress.getLoopbackAddress());
      s.activate(new Session.Identity("t1", "t1:u" + i, "d1", "ios", "1.0", Set.of()), "C" + i, 0L, f.wallNow(),
          f.wallNow() + 3_600, 0, List.of());
      sessions.add(s);
    }
    Session other = f.tracker.admit(new EmbeddedChannel(), f.loop, InetAddress.getLoopbackAddress());
    other.activate(new Session.Identity("t2", "t2:u", "d1", "ios", "1.0", Set.of()), "O", 0L, f.wallNow(),
        f.wallNow() + 3_600, 0, List.of());
    f.cells.put("t1", "cell-2");
  }

  private void tick() {
    migrator.tick();
    f.channel.runPendingTasks();
  }

  private long draining() {
    return sessions.stream().filter(s -> s.state() == SessionState.DRAINING).count();
  }

  @Test
  void drainsTwoPercentPerTickWithReBootstrap() throws Exception {
    migrator.start("t1");

    tick();

    assertThat(draining()).isEqualTo(2);
    Session drained = sessions.stream().filter(s -> s.state() == SessionState.DRAINING).findFirst().orElseThrow();
    EmbeddedChannel ch = (EmbeddedChannel) drained.channel();
    Frame frame = ch.readOutbound();
    assertThat(frame.getType()).isEqualTo(FrameType.FRAME_TYPE_RECONNECT);
    Reconnect r = Reconnect.parseFrom(frame.getPayload());
    assertThat(r.getReBootstrap()).isTrue();
    assertThat(r.getDelayMs()).isEqualTo(1_500);

    tick();
    assertThat(draining()).isEqualTo(4);
  }

  @Test
  void stopsWhenTenantMappedBack() {
    migrator.start("t1");
    tick();
    f.cells.put("t1", "cell-1");

    tick();

    assertThat(draining()).isEqualTo(2);
    assertThat(migrator.migrating()).isEmpty();
  }

  @Test
  void finishesWhenNoActiveSessionLeft() {
    migrator.start("t1");
    for (int i = 0; i < 60; i++) {
      tick();
    }

    assertThat(draining()).isEqualTo(100);
    assertThat(migrator.migrating()).isEmpty();
  }
}
