package com.pangate.gateway.downstream;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.contracts.v1.ControlOp;
import com.pangate.contracts.v1.Kick;
import com.pangate.contracts.v1.RefreshConfig;
import com.pangate.contracts.v1.Scope;
import com.pangate.contracts.v1.TenantCfg;
import com.pangate.contracts.v1.TenantLimits;
import com.pangate.contracts.v1.Topics;
import com.pangate.gateway.protocol.CloseCode;
import com.pangate.gateway.session.Session;
import com.pangate.gateway.session.SessionState;
import com.pangate.gateway.test.LoopFixture;
import io.netty.channel.embedded.EmbeddedChannel;
import java.net.InetAddress;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class ControlPushHandlerTest {

  private final LoopFixture f = new LoopFixture();
  private final ControlPushHandler handler = new ControlPushHandler(f.principals, f.tenants, f.metrics);

  private Session session(String principal, long connectedAtMs) {
    EmbeddedChannel ch = new EmbeddedChannel();
    Session s = f.tracker.admit(ch, f.loop, InetAddress.getLoopbackAddress());
    s.activate(new Session.Identity("t1", principal, "d", "ios", "1.0", Set.of()), f.tracker.newConnId(s),
        connectedAtMs, f.wallNow(), f.wallNow() + 3600, 1, List.of());
    f.tracker.onActivated(s);
    return s;
  }

  private static ControlOp subscribe(String... topics) {
    return ControlOp.newBuilder().setSubscribe(Topics.newBuilder().addAllTopics(List.of(topics))).build();
  }

  @Test
  void controlSubscribeAddsOnlyTenantTopicsIgnoringPatterns() {
    Session a = session("t1:u1", 1);
    Session b = session("t1:u1", 2);
    handler.apply(f.loop, "t1:u1", subscribe("t1:any", "t2:other"));
    assertThat(f.loop.topics().snapshot("t1:any")).containsExactly(a, b);
    assertThat(f.loop.topics().snapshot("t2:other")).isEmpty();
    assertThat(f.meters.counter("gw.subscribe.rejected", "reason", "control").count()).isEqualTo(2);
    assertThat(f.meters.counter("gw.control.push", "op", "subscribe").count()).isEqualTo(1);
  }

  @Test
  void controlSubscribeStopsAtTopicsPerConn() {
    f.tenants.apply(TenantCfg.newBuilder().setScope(Scope.SCOPE_TENANT).setScopeId("t1")
        .setLimits(TenantLimits.newBuilder().setTopicsPerConn(2)).build());
    Session a = session("t1:u1", 1);
    handler.apply(f.loop, "t1:u1", subscribe("t1:a", "t1:b", "t1:c"));
    assertThat(a.topics()).containsExactly("t1:a", "t1:b");
  }

  @Test
  void controlUnsubscribeRemoves() {
    Session a = session("t1:u1", 1);
    handler.apply(f.loop, "t1:u1", subscribe("t1:a", "t1:b"));
    handler.apply(f.loop, "t1:u1",
        ControlOp.newBuilder().setUnsubscribe(Topics.newBuilder().addTopics("t1:a")).build());
    assertThat(a.topics()).containsExactly("t1:b");
  }

  @Test
  void kickClosesOlderSessions4012() {
    Session old = session("t1:u1", 100);
    Session fresh = session("t1:u1", 200);
    handler.apply(f.loop, "t1:u1", ControlOp.newBuilder()
        .setKick(Kick.newBuilder().setNewConnId(fresh.connId()).setConnectedAtMs(200)).build());
    assertThat(old.state()).isEqualTo(SessionState.CLOSED);
    assertThat(old.closeCode()).isEqualTo(CloseCode.DUPLICATE_SESSION);
    assertThat(fresh.state()).isEqualTo(SessionState.ACTIVE);
    assertThat(f.meters.counter("gw.control.push", "op", "kick").count()).isEqualTo(1);
  }

  @Test
  void kickSparesNewConnAndNewerSessions() {
    Session a = session("t1:u1", 100);
    Session b = session("t1:u1", 200);
    Session c = session("t1:u1", 300);
    handler.kick(f.loop, "t1:u1", b.connId(), 200);
    assertThat(a.closeCode()).isEqualTo(CloseCode.DUPLICATE_SESSION);
    assertThat(b.state()).isEqualTo(SessionState.ACTIVE);
    assertThat(c.state()).isEqualTo(SessionState.ACTIVE);
  }

  @Test
  void otherPrincipalsAndLoopsUntouched() {
    Session other = session("t1:u2", 1);
    handler.kick(f.loop, "t1:u1", "X", 1_000);
    handler.apply(f.loop, "t1:u1", subscribe("t1:a"));
    assertThat(other.state()).isEqualTo(SessionState.ACTIVE);
    assertThat(other.topics()).isNull();
  }

  @Test
  void refreshConfigCountsOnly() {
    Session a = session("t1:u1", 1);
    handler.apply(f.loop, "t1:u1", ControlOp.newBuilder().setRefreshConfig(RefreshConfig.getDefaultInstance()).build());
    assertThat(a.state()).isEqualTo(SessionState.ACTIVE);
    assertThat(f.meters.counter("gw.control.push", "op", "refresh_config").count()).isEqualTo(1);
  }
}
