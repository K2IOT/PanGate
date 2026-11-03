package com.pangate.gateway.control;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.contracts.v1.KillSwitches;
import com.pangate.contracts.v1.Scope;
import com.pangate.contracts.v1.TenantCfg;
import com.pangate.gateway.test.TestLimits;
import org.junit.jupiter.api.Test;

class ControlStateTest {

  private final RevocationList revocations = new RevocationList();
  private final CellMapView cells = new CellMapView("cell-1");
  private final TenantLimitsStore tenants = new TenantLimitsStore("cell-1", TestLimits::defaults);
  private final ControlState state = new ControlState(revocations, cells, tenants, () -> 1_000);

  @Test
  void dispatchesToStores() {
    state.onRevoke("p:t1:u1", 5_000);
    state.onCellMap("t2", "cell-2");
    state.onTenantCfg(TenantCfg.newBuilder().setScope(Scope.SCOPE_TENANT).setScopeId("t3")
        .setSwitches(KillSwitches.newBuilder().setTenantBlocked(true)).build());

    assertThat(revocations.isRevoked("t1:u1", "d", 0, 1_000)).isTrue();
    assertThat(cells.servesTenant("t2")).isFalse();
    assertThat(tenants.policy("t3").blocked()).isTrue();
  }

  @Test
  void ignoresExpiredRevokes() {
    state.onRevoke("p:t1:u1", 1_000);

    assertThat(revocations.size()).isZero();
  }

  @Test
  void tombstonesRemoveState() {
    state.onRevoke("p:t1:u1", 5_000);
    state.onCellMap("t2", "cell-2");
    state.onTenantCfg(TenantCfg.newBuilder().setScope(Scope.SCOPE_TENANT).setScopeId("t3")
        .setSwitches(KillSwitches.newBuilder().setTenantBlocked(true)).build());

    state.onRevokeRemoved("p:t1:u1");
    state.onCellMapRemoved("t2");
    state.onTenantCfgRemoved(Scope.SCOPE_TENANT, "t3");

    assertThat(revocations.size()).isZero();
    assertThat(cells.servesTenant("t2")).isTrue();
    assertThat(tenants.policy("t3").blocked()).isFalse();
  }
}
