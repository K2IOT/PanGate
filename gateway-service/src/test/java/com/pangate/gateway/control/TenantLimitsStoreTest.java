package com.pangate.gateway.control;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.contracts.v1.KillSwitches;
import com.pangate.contracts.v1.Scope;
import com.pangate.contracts.v1.TenantCfg;
import com.pangate.contracts.v1.TenantLimits;
import com.pangate.gateway.test.TestLimits;
import org.junit.jupiter.api.Test;

class TenantLimitsStoreTest {

  private final TenantLimitsStore store = new TenantLimitsStore("cell-1", TestLimits::defaults);

  private static TenantCfg heartbeat(Scope scope, String id, int sec) {
    return TenantCfg.newBuilder().setScope(scope).setScopeId(id)
        .setSwitches(KillSwitches.newBuilder().setHeartbeatIntervalSec(sec)).build();
  }

  @Test
  void defaultsComeFromGatewayLimits() {
    TenantPolicy p = store.policy("t1");

    assertThat(p.heartbeatSec()).isEqualTo(30);
    assertThat(p.idleTimeoutSec()).isEqualTo(90);
    assertThat(p.inboundRate()).isEqualTo(50);
    assertThat(p.inboundBurst()).isEqualTo(100);
    assertThat(p.topicsPerConn()).isEqualTo(100);
    assertThat(p.telemetryEnabled()).isTrue();
    assertThat(p.broadcastEnabled()).isTrue();
    assertThat(p.blocked()).isFalse();
    assertThat(p.minSdkVersion()).isEmpty();
    assertThat(p.originAllowlist()).isEmpty();
  }

  @Test
  void tenantOverridesCellOverridesFleet() {
    store.apply(heartbeat(Scope.SCOPE_FLEET, "", 60));
    store.apply(heartbeat(Scope.SCOPE_CELL, "cell-1", 45));
    store.apply(heartbeat(Scope.SCOPE_CELL, "cell-2", 5));
    store.apply(heartbeat(Scope.SCOPE_TENANT, "t1", 20));

    assertThat(store.policy("t1").heartbeatSec()).isEqualTo(20);
    assertThat(store.policy("t2").heartbeatSec()).isEqualTo(45);
  }

  @Test
  void absentOptionalKeepsLowerScope() {
    store.apply(heartbeat(Scope.SCOPE_FLEET, "", 60));
    store.apply(TenantCfg.newBuilder().setScope(Scope.SCOPE_TENANT).setScopeId("t1")
        .setSwitches(KillSwitches.newBuilder().setTenantBlocked(true))
        .setLimits(TenantLimits.newBuilder().setMinSdkVersion("2.0").addOriginAllowlist("https://a")).build());

    TenantPolicy p = store.policy("t1");
    assertThat(p.heartbeatSec()).isEqualTo(60);
    assertThat(p.blocked()).isTrue();
    assertThat(p.minSdkVersion()).isEqualTo("2.0");
    assertThat(p.originAllowlist()).containsExactly("https://a");
  }

  @Test
  void removeRestoresLowerScope() {
    store.apply(heartbeat(Scope.SCOPE_FLEET, "", 60));
    store.apply(heartbeat(Scope.SCOPE_TENANT, "t1", 20));

    store.remove(Scope.SCOPE_TENANT, "t1");
    assertThat(store.policy("t1").heartbeatSec()).isEqualTo(60);
    store.remove(Scope.SCOPE_FLEET, "");
    assertThat(store.policy("t1").heartbeatSec()).isEqualTo(30);
  }

  @Test
  void unknownTenantGetsFleetCellPolicy() {
    store.apply(TenantCfg.newBuilder().setScope(Scope.SCOPE_CELL).setScopeId("cell-1")
        .setSwitches(KillSwitches.newBuilder().setUpstreamTelemetryEnabled(false)).build());

    assertThat(store.policy("never-seen").telemetryEnabled()).isFalse();
  }

  @Test
  void zeroHeartbeatIsIgnored() {
    store.apply(heartbeat(Scope.SCOPE_TENANT, "t1", 0));

    assertThat(store.policy("t1").heartbeatSec()).isEqualTo(30);
  }
}
