package com.pangate.gateway.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.contracts.v1.Frame;
import com.pangate.contracts.v1.FrameType;
import com.pangate.contracts.v1.KillSwitches;
import com.pangate.contracts.v1.Reconnect;
import com.pangate.contracts.v1.Scope;
import com.pangate.contracts.v1.TenantCfg;
import com.pangate.gateway.config.GatewayProperties;
import com.pangate.gateway.control.TenantPolicy;
import com.pangate.gateway.session.Session;
import com.pangate.gateway.session.SessionState;
import com.pangate.gateway.test.LoopFixture;
import com.pangate.gateway.test.TestLimits;
import java.net.InetAddress;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.SplittableRandom;
import org.junit.jupiter.api.Test;

class RecyclerTest {

  private final LoopFixture f = new LoopFixture();

  private Recycler recycler(Duration min, Duration max) {
    Recycler r = new Recycler(new GatewayProperties.Recycling(min, max, Duration.ofSeconds(30)), f.node, f.tenants,
        new SplittableRandom(7), f.metrics);
    f.loop.recycler(r);
    return r;
  }

  private Session active(String tenant) {
    Session s = f.tracker.admit(f.channel, f.loop, InetAddress.getLoopbackAddress());
    s.activate(new Session.Identity(tenant, tenant + ":u1", "d1", "ios", "1.0", Set.of()), f.tracker.newConnId(s), 0L,
        f.wallNow(), f.wallNow() + 86_400, 7, List.of());
    return s;
  }

  /** Advances second by second, keeping the session's heartbeat alive. */
  private void runFor(Session s, long seconds) {
    for (long i = 0; i < seconds; i++) {
      s.touch(f.now());
      f.advanceTo(f.now() + 1);
    }
  }

  private Reconnect reconnect() throws Exception {
    Object o;
    while ((o = f.channel.readOutbound()) != null) {
      if (o instanceof Frame fr && fr.getType() == FrameType.FRAME_TYPE_RECONNECT) {
        return Reconnect.parseFrom(fr.getPayload());
      }
    }
    return null;
  }

  @Test
  void firstRecycleIsBetweenMinAndMax() {
    Recycler r = recycler(Duration.ofMinutes(30), Duration.ofMinutes(90));
    for (int i = 0; i < 100; i++) {
      assertThat(r.firstRecycleAtSec(1000)).isBetween(1000L + 1800, 1000L + 5400);
    }
  }

  @Test
  void dueSessionGetsReconnectAndDrains() throws Exception {
    recycler(Duration.ofSeconds(10), Duration.ofSeconds(10));
    Session s = active("t1");
    runFor(s, 9);
    assertThat(reconnect()).isNull();
    runFor(s, 1);
    Reconnect r = reconnect();
    assertThat(r).isNotNull();
    assertThat(r.getDelayMs()).isBetween(0, 5000);
    assertThat(r.getReBootstrap()).isFalse();
    assertThat(s.state()).isEqualTo(SessionState.DRAINING);
    assertThat(f.meters.counter("gw.recycle.reconnects").count()).isEqualTo(1);
    runFor(s, 5 + 30);  // delay + grace
    assertThat(s.state()).isEqualTo(SessionState.CLOSED);
  }

  @Test
  void shedLevel3PostponesBy60To300s() throws Exception {
    recycler(Duration.ofSeconds(10), Duration.ofSeconds(10));
    Session s = active("t1");
    f.node.shedLevel(3);
    runFor(s, 10 + 59);
    assertThat(reconnect()).isNull();
    assertThat(s.state()).isEqualTo(SessionState.ACTIVE);
    f.node.shedLevel(0);
    runFor(s, 300);
    assertThat(reconnect()).isNotNull();
  }

  @Test
  void tenantWithRecycleDisabledIsPostponed() throws Exception {
    f.tenants.apply(TenantCfg.newBuilder().setScope(Scope.SCOPE_TENANT).setScopeId("t2")
        .setSwitches(KillSwitches.newBuilder().setRecycleEnabled(false)).build());
    recycler(Duration.ofSeconds(10), Duration.ofSeconds(10));
    Session s = active("t2");
    runFor(s, 400);
    assertThat(reconnect()).isNull();
    assertThat(s.state()).isEqualTo(SessionState.ACTIVE);
  }

  @Test
  void defaultPolicyHasRecycleEnabled() {
    assertThat(TenantPolicy.defaults(TestLimits.defaults()).recycleEnabled()).isTrue();
  }
}
