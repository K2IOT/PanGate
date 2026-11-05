package com.pangate.gateway.control;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.contracts.v1.KillSwitches;
import com.pangate.contracts.v1.Scope;
import com.pangate.contracts.v1.TenantCfg;
import com.pangate.gateway.protocol.ChannelAttrs;
import com.pangate.gateway.session.Session;
import com.pangate.gateway.session.SessionState;
import com.pangate.gateway.test.LoopFixture;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class ControlEnforcerTest {

  private final LoopFixture f = new LoopFixture();
  private final List<EmbeddedChannel> channels = new ArrayList<>();

  @AfterEach
  void releaseOutbound() {  // close frames written to the sessions' channels
    channels.forEach(EmbeddedChannel::finishAndReleaseAll);
    f.channel.finishAndReleaseAll();
  }
  private final List<String> migrations = new ArrayList<>();
  private final TenantMigrator migrator = new TenantMigrator(f.loops, f.cells, () -> 0L) {
    @Override
    public void start(String tenant) {
      migrations.add("start " + tenant);
    }

    @Override
    public void stop(String tenant) {
      migrations.add("stop " + tenant);
    }
  };
  private final ControlState state = new ControlState(f.revocations, f.cells, f.tenants, () -> 0L);
  private final ControlEnforcer enforcer = new ControlEnforcer(f.principals, f.loops, f.tenants, f.cells, migrator,
      () -> 0L);

  private Session session(String tenant, String principal, String device, long jtiHash) {
    EmbeddedChannel ch = new EmbeddedChannel();
    channels.add(ch);
    ch.attr(ChannelAttrs.TRANSPORT).set(ChannelAttrs.TransportKind.WS);
    ch.attr(ChannelAttrs.PROTOCOL_READY).set(true);
    Session s = f.tracker.admit(ch, f.loop, InetAddress.getLoopbackAddress());
    s.activate(new Session.Identity(tenant, principal, device, "ios", "1.0", Set.of()), principal + "/" + device, 0L,
        f.wallNow(), f.wallNow() + 3_600, jtiHash, List.of());
    f.tracker.onActivated(s);
    return s;
  }

  private void both(java.util.function.Consumer<ControlEventHandler> event) {
    event.accept(state);
    event.accept(enforcer);
    f.channel.runPendingTasks();
  }

  private static CloseWebSocketFrame closeFrame(Session s) {
    EmbeddedChannel ch = (EmbeddedChannel) s.channel();
    ch.runPendingTasks();
    return ch.readOutbound();
  }

  @Test
  void principalRevokeClosesAllItsSessionsWith4003() {
    Session a = session("t1", "t1:u1", "d1", 1);
    Session b = session("t1", "t1:u1", "d2", 2);
    Session other = session("t1", "t1:u2", "d1", 3);

    both(h -> h.onRevoke(RevocationList.principalKey("t1:u1"), Long.MAX_VALUE));

    assertThat(a.closeCode().code()).isEqualTo(4003);
    assertThat(b.closeCode().code()).isEqualTo(4003);
    assertThat(other.state()).isEqualTo(SessionState.ACTIVE);
  }

  @Test
  void deviceRevokeClosesOnlyThatDevice() {
    Session a = session("t1", "t1:u1", "d1", 1);
    Session b = session("t1", "t1:u1", "d2", 2);

    both(h -> h.onRevoke(RevocationList.deviceKey("t1:u1", "d2"), Long.MAX_VALUE));

    assertThat(a.state()).isEqualTo(SessionState.ACTIVE);
    assertThat(b.closeCode().code()).isEqualTo(4003);
  }

  @Test
  void jtiRevokeClosesOnlyMatchingSession() {
    Session a = session("t1", "t1:u1", "d1", 11);
    Session b = session("t1", "t1:u2", "d1", 22);

    both(h -> h.onRevoke(RevocationList.jtiKey(22), Long.MAX_VALUE));

    assertThat(a.state()).isEqualTo(SessionState.ACTIVE);
    assertThat(b.closeCode().code()).isEqualTo(4003);
  }

  @Test
  void expiredRevokeIsIgnored() {
    Session a = session("t1", "t1:u1", "d1", 1);

    both(h -> h.onRevoke(RevocationList.principalKey("t1:u1"), -1));

    assertThat(a.state()).isEqualTo(SessionState.ACTIVE);
  }

  @Test
  void tenantBlockedClosesOpenSessionsWith4013RetryAfter300s() {
    Session a = session("t1", "t1:u1", "d1", 1);
    Session other = session("t2", "t2:u1", "d1", 2);

    both(h -> h.onTenantCfg(TenantCfg.newBuilder().setScope(Scope.SCOPE_TENANT).setScopeId("t1")
        .setSwitches(KillSwitches.newBuilder().setTenantBlocked(true)).build()));

    assertThat(a.closeCode().code()).isEqualTo(4013);
    CloseWebSocketFrame frame = closeFrame(a);
    assertThat(frame.statusCode()).isEqualTo(4013);
    assertThat(frame.reasonText()).contains("retryAfter=300000");
    frame.release();
    assertThat(other.state()).isEqualTo(SessionState.ACTIVE);
  }

  @Test
  void cellMapMovesStartAndStopMigration() {
    both(h -> h.onCellMap("t1", "cell-2"));
    both(h -> h.onCellMap("t1", "cell-1"));
    both(h -> h.onCellMap("t3", "cell-9"));
    both(h -> h.onCellMapRemoved("t3"));

    assertThat(migrations).containsExactly("start t1", "stop t1", "start t3", "stop t3");
  }
  @Test
  void heartbeatChangeTakesEffectOnOpenSessions() {
    Session a = session("t1", "t1:u1", "d1", 1);
    Session other = session("t2", "t2:u1", "d1", 2);
    long start = f.now();

    both(h -> h.onTenantCfg(TenantCfg.newBuilder().setScope(Scope.SCOPE_TENANT).setScopeId("t1")
        .setSwitches(KillSwitches.newBuilder().setHeartbeatIntervalSec(10)).build()));
    f.advanceTo(start + 30);

    assertThat(a.closeReason()).isEqualTo("HEARTBEAT_TIMEOUT");
    assertThat(other.state()).isEqualTo(SessionState.ACTIVE);
  }

  @Test
  void fleetHeartbeatChangeReschedulesEverySession() {
    Session a = session("t1", "t1:u1", "d1", 1);
    long start = f.now();

    both(h -> h.onTenantCfg(TenantCfg.newBuilder().setScope(Scope.SCOPE_FLEET)
        .setSwitches(KillSwitches.newBuilder().setHeartbeatIntervalSec(5)).build()));
    f.advanceTo(start + 15);

    assertThat(a.closeReason()).isEqualTo("HEARTBEAT_TIMEOUT");
  }
}
