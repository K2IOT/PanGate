package com.pangate.pushapi.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.pangate.contracts.v1.ControlEvent;
import com.pangate.contracts.v1.DeviceRef;
import com.pangate.contracts.v1.KillSwitches;
import com.pangate.contracts.v1.Revoke;
import com.pangate.contracts.v1.Scope;
import com.pangate.contracts.v1.TenantCfg;
import com.pangate.contracts.v1.TenantLimits;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class ControlStateTest {

  final AtomicLong now = new AtomicLong(10_000);
  static final TenantPolicy DEFAULTS = new TenantPolicy(false, true, 1000, 2000, 10);
  final ControlState s = new ControlState("c1", DEFAULTS, now::get);

  static byte[] revoke(Revoke r, long issuedAt) {
    return ControlEvent.newBuilder().setRevoke(r).setIssuedAtMs(issuedAt).build().toByteArray();
  }

  static byte[] cfg(Scope scope, String id, TenantLimits l, KillSwitches k) {
    TenantCfg.Builder c = TenantCfg.newBuilder().setScope(scope).setScopeId(id);
    if (l != null) {
      c.setLimits(l);
    }
    if (k != null) {
      c.setSwitches(k);
    }
    return ControlEvent.newBuilder().setTenantCfg(c).build().toByteArray();
  }

  @Test
  void jtiRevokedUntilExpiry() {
    s.apply("revoke:j:J1", revoke(Revoke.newBuilder().setJti("J1").setExpiresAtMs(20_000).build(), 9_000));
    assertThat(s.jtiRevoked("J1")).isTrue();
    assertThat(s.jtiRevoked("J2")).isFalse();
    now.set(20_001);
    assertThat(s.jtiRevoked("J1")).isFalse();
    s.prune();
    assertThat(s.revocations()).isZero();
  }

  /** Review Focus 3. */
  @Test
  void clientBlockOnlyCoversTokensIssuedBeforeTheEvent() {
    s.apply("revoke:c:t1-backend", revoke(Revoke.newBuilder().setClientId("t1-backend").setExpiresAtMs(50_000).build(),
        1_000));
    assertThat(s.clientBlocked("t1-backend", 999)).isTrue();
    assertThat(s.clientBlocked("t1-backend", 1_001)).isFalse();
    assertThat(s.clientBlocked("t1-other", 999)).isFalse();
  }

  @Test
  void tombstoneClears() {
    s.apply("revoke:j:J1", revoke(Revoke.newBuilder().setJti("J1").build(), 9_000));
    assertThat(s.jtiRevoked("J1")).isTrue();
    s.apply("revoke:j:J1", null);
    assertThat(s.jtiRevoked("J1")).isFalse();
    s.apply("tenantcfg:tenant:t1", cfg(Scope.SCOPE_TENANT, "t1", null,
        KillSwitches.newBuilder().setTenantBlocked(true).build()));
    s.apply("tenantcfg:tenant:t1", new byte[0]);
    assertThat(s.policy("t1").blocked()).isFalse();
  }

  @Test
  void policyMergesFleetCellTenant() {
    s.apply("tenantcfg:fleet", cfg(Scope.SCOPE_FLEET, "", TenantLimits.newBuilder().setPushRate(500).build(), null));
    s.apply("tenantcfg:cell:c1", cfg(Scope.SCOPE_CELL, "c1", TenantLimits.newBuilder().setPushBurst(700).build(), null));
    s.apply("tenantcfg:cell:c2", cfg(Scope.SCOPE_CELL, "c2", TenantLimits.newBuilder().setBroadcastRate(1).build(),
        null));
    s.apply("tenantcfg:tenant:t1", cfg(Scope.SCOPE_TENANT, "t1", TenantLimits.newBuilder().setBroadcastRate(3).build(),
        null));
    assertThat(s.policy("t1")).isEqualTo(new TenantPolicy(false, true, 500, 700, 3));
    assertThat(s.policy("t2")).isEqualTo(new TenantPolicy(false, true, 500, 700, 10));
  }

  @Test
  void tenantBlockedAndBroadcastSwitch() {
    s.apply("tenantcfg:tenant:t1", cfg(Scope.SCOPE_TENANT, "t1", null,
        KillSwitches.newBuilder().setTenantBlocked(true).setBroadcastEnabled(false).build()));
    assertThat(s.policy("t1").blocked()).isTrue();
    assertThat(s.policy("t1").broadcastEnabled()).isFalse();
    assertThat(s.policy("t2").blocked()).isFalse();
  }

  @Test
  void principalAndDeviceRevokesAreIgnored() {
    s.apply("revoke:p:t1:u1", revoke(Revoke.newBuilder().setPrincipalId("t1:u1").build(), 1));
    s.apply("revoke:d:t1:u1/d1", revoke(Revoke.newBuilder().setDevice(DeviceRef.newBuilder().setPrincipalId("t1:u1")
        .setDeviceId("d1")).build(), 1));
    s.apply("cellmap:t1", new byte[] {1});
    s.apply("trace:t1:u1", new byte[] {1});
    assertThat(s.revocations()).isZero();
  }

  @Test
  void malformedValueThrows() {
    assertThatThrownBy(() -> s.apply("revoke:j:J1", new byte[] {(byte) 0xff, 1}))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
