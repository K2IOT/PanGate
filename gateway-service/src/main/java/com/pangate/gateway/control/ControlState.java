package com.pangate.gateway.control;

import com.pangate.contracts.v1.Scope;
import com.pangate.contracts.v1.TenantCfg;
import java.util.function.LongSupplier;

/** Applies gw.control events to the node's control stores; runs before any enforcement handler. */
public final class ControlState implements ControlEventHandler {

  private final RevocationList revocations;
  private final CellMapView cells;
  private final TenantLimitsStore tenants;
  private final LongSupplier wallMillis;

  public ControlState(RevocationList revocations, CellMapView cells, TenantLimitsStore tenants,
      LongSupplier wallMillis) {
    this.revocations = revocations;
    this.cells = cells;
    this.tenants = tenants;
    this.wallMillis = wallMillis;
  }

  @Override
  public void onRevoke(String key, long expiresAtMs) {
    if (expiresAtMs > wallMillis.getAsLong()) {
      revocations.add(key, expiresAtMs);
    }
  }

  @Override
  public void onRevokeRemoved(String key) {
    revocations.remove(key);
  }

  @Override
  public void onCellMap(String tenant, String cell) {
    cells.put(tenant, cell);
  }

  @Override
  public void onCellMapRemoved(String tenant) {
    cells.remove(tenant);
  }

  @Override
  public void onTenantCfg(TenantCfg cfg) {
    tenants.apply(cfg);
  }

  @Override
  public void onTenantCfgRemoved(Scope scope, String id) {
    tenants.remove(scope, id);
  }
}
