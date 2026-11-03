package com.pangate.gateway.control;

import com.pangate.contracts.v1.Scope;
import com.pangate.contracts.v1.TenantCfg;

/** Receives gw.control events in log order on the gw-ctl thread (spec §6.8). Handlers must not block for long. */
public interface ControlEventHandler {

  /** {@code revocationKey} is in {@link RevocationList} form. */
  default void onRevoke(String revocationKey, long expiresAtMs) {}

  default void onRevokeRemoved(String revocationKey) {}

  default void onCellMap(String tenant, String cell) {}

  default void onCellMapRemoved(String tenant) {}

  default void onTenantCfg(TenantCfg cfg) {}

  default void onTenantCfgRemoved(Scope scope, String id) {}
}
