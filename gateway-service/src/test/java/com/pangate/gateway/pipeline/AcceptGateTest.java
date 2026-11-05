package com.pangate.gateway.pipeline;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.gateway.config.GatewayProperties;
import com.pangate.gateway.server.NodeState;
import com.pangate.gateway.session.Session;
import com.pangate.gateway.shed.AdmissionController;
import com.pangate.gateway.test.LoopFixture;
import java.net.InetAddress;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class AcceptGateTest {

  private final LoopFixture f = new LoopFixture();
  private final NodeState node = new NodeState();
  private AdmissionController admission =
      new AdmissionController(new GatewayProperties.Admission(5000, 10000, 500, Duration.ofMinutes(2)), 1, System::nanoTime);

  private AcceptGate gate() {
    return new AcceptGate(node, f.loops, f.limits, f.ips, admission, f.tracker, f.metrics);
  }

  private void activate() {
    f.channel.pipeline().addLast(gate());
    f.channel.pipeline().fireChannelActive();
    f.channel.runPendingTasks();
  }

  private double rejected(String reason) {
    var c = f.meters.find("gw.admission.rejected").tag("reason", reason).counter();
    return c == null ? 0 : c.count();
  }

  @Test
  void rejectsWhenDraining() {
    node.draining(true);

    activate();

    assertThat(f.channel.isOpen()).isFalse();
    assertThat(rejected("draining")).isEqualTo(1);
  }

  @Test
  void rejectsAboveHardCapPerLoop() {
    for (int i = 0; i < 700_000; i++) {
      f.loop.incrementActive();
    }

    activate();

    assertThat(f.channel.isOpen()).isFalse();
    assertThat(rejected("hard_cap")).isEqualTo(1);
  }

  @Test
  void rejectsPerIpWithReason() {
    for (int i = 0; i < 200; i++) {
      f.ips.tryIncrement(InetAddress.getLoopbackAddress(), 200);
    }

    activate();

    assertThat(f.channel.isOpen()).isFalse();
    assertThat(rejected("per_ip")).isEqualTo(1);
  }

  @Test
  void rejectsOnRateWithoutLeakingIpCount() {
    admission = new AdmissionController(new GatewayProperties.Admission(1, 1, 1, Duration.ofMinutes(2)), 1, () -> 0L);
    admission.tryAdmit(0);

    activate();

    assertThat(f.channel.isOpen()).isFalse();
    assertThat(rejected("rate")).isEqualTo(1);
    assertThat(f.ips.trackedKeys()).isZero();
  }

  @Test
  void admittedChannelTrackedAndCleanedOnClose() {
    activate();

    assertThat(f.channel.isOpen()).isTrue();
    assertThat(f.channel.attr(Session.KEY).get()).isNotNull();
    assertThat(f.loop.activeCount()).isEqualTo(1);
    assertThat(f.channel.pipeline().get(AcceptGate.class)).isNull();

    f.channel.close();
    f.channel.runPendingTasks();

    assertThat(f.loop.activeCount()).isZero();
    assertThat(f.ips.trackedKeys()).isZero();
  }

  @Test
  void tcpGateRejectsAtShedLevel3ButWsGateLeavesItToTheUpgradeGuard() {
    node.shedLevel(3);
    f.channel.pipeline().addLast(gate().forTcp());
    f.channel.pipeline().fireChannelActive();
    f.channel.runPendingTasks();
    assertThat(f.channel.isOpen()).isFalse();
    assertThat(rejected("shed")).isEqualTo(1);

    io.netty.channel.embedded.EmbeddedChannel ws = new io.netty.channel.embedded.EmbeddedChannel();
    ws.pipeline().addLast(gate());
    ws.pipeline().fireChannelActive();
    assertThat(ws.isOpen()).isTrue();
  }
}
