package com.pangate.gateway.registry;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.gateway.downstream.ControlPushHandler;
import com.pangate.gateway.protocol.CloseCode;
import com.pangate.gateway.session.Session;
import com.pangate.gateway.session.SessionState;
import com.pangate.gateway.test.LoopFixture;
import io.netty.channel.embedded.EmbeddedChannel;
import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;

class SingleSessionKickerTest {

  record Kick(String tenant, String principal, String newConnId, long connectedAtMs) {}

  private final LoopFixture f = new LoopFixture();  // node n1, epoch 7
  private final List<Kick> remote = new CopyOnWriteArrayList<>();
  private final KickClient client = (t, p, c, at) -> remote.add(new Kick(t, p, c, at));
  private final SingleSessionKicker kicker = new SingleSessionKicker(f.principals, f.loops, f.node,
      new ControlPushHandler(f.principals, f.tenants, f.metrics), client);

  private Session session(long connectedAtMs) {
    EmbeddedChannel ch = new EmbeddedChannel();
    Session s = f.tracker.admit(ch, f.loop, InetAddress.getLoopbackAddress());
    s.activate(new Session.Identity("t1", "t1:u1", "d", "ios", "1.0", Set.of()), f.tracker.newConnId(s),
        connectedAtMs, f.wallNow(), f.wallNow() + 3600, 1, List.of());
    f.tracker.onActivated(s);
    return s;
  }

  @Test
  void localPreviousEntryKickedRemoteSentToPushApi() {
    Session old = session(100);
    Session fresh = session(200);
    kicker.accept(fresh, Map.of(old.connId(), "n1|7|100|ios", "C9", "n2|3|50|ios", "C8", "n3|1|60|web"));
    f.channel.runPendingTasks();
    assertThat(old.state()).isEqualTo(SessionState.CLOSED);
    assertThat(old.closeCode()).isEqualTo(CloseCode.DUPLICATE_SESSION);
    assertThat(fresh.state()).isEqualTo(SessionState.ACTIVE);
    assertThat(remote).containsExactly(new Kick("t1", "t1:u1", fresh.connId(), 200));
  }

  @Test
  void sameNodeOlderEpochIsRemote() {
    Session fresh = session(200);
    kicker.accept(fresh, Map.of("C1", "n1|6|100|ios"));
    assertThat(remote).hasSize(1);
  }

  @Test
  void malformedEntryIgnored() {
    Session fresh = session(200);
    kicker.accept(fresh, Map.of("C1", "garbage", "C2", "n2|x|1|ios"));
    f.channel.runPendingTasks();
    assertThat(remote).isEmpty();
    assertThat(fresh.state()).isEqualTo(SessionState.ACTIVE);
  }

  @Test
  void disabledPushApiOnlyWarns() {
    Session fresh = session(200);
    new SingleSessionKicker(f.principals, f.loops, f.node,
        new ControlPushHandler(f.principals, f.tenants, f.metrics), KickClient.disabled())
        .accept(fresh, Map.of("C1", "n2|1|1|ios"));
    assertThat(fresh.state()).isEqualTo(SessionState.ACTIVE);
  }
}
