package com.pangate.gateway.control;

import com.pangate.contracts.v1.Scope;
import com.pangate.gateway.auth.ResumeTicketService;

/** Parses gw.control record keys (contract in control.proto); used for tombstones, whose value is empty. */
public final class ControlKeys {

  public sealed interface Parsed permits RevokeKey, CellMapKey, TenantCfgKey, Unknown {}

  /** {@code revocationKey} is already in {@link RevocationList} form (jti hashed). */
  public record RevokeKey(String revocationKey) implements Parsed {}

  public record CellMapKey(String tenant) implements Parsed {}

  public record TenantCfgKey(Scope scope, String id) implements Parsed {}

  public record Unknown(String key) implements Parsed {}

  private ControlKeys() {}

  public static Parsed parse(String key) {
    if (key == null) {
      return new Unknown(null);
    }
    if (key.startsWith("revoke:p:") && key.length() > 9) {
      return new RevokeKey(RevocationList.principalKey(key.substring(9)));
    }
    if (key.startsWith("revoke:d:") && key.indexOf('/', 9) > 9 && !key.endsWith("/")) {
      int slash = key.lastIndexOf('/');
      return new RevokeKey(RevocationList.deviceKey(key.substring(9, slash), key.substring(slash + 1)));
    }
    if (key.startsWith("revoke:j:") && key.length() > 9) {
      return new RevokeKey(RevocationList.jtiKey(ResumeTicketService.jtiHash(key.substring(9))));
    }
    if (key.startsWith("cellmap:") && key.length() > 8) {
      return new CellMapKey(key.substring(8));
    }
    if (key.equals("tenantcfg:fleet")) {
      return new TenantCfgKey(Scope.SCOPE_FLEET, "");
    }
    if (key.startsWith("tenantcfg:cell:") && key.length() > 15) {
      return new TenantCfgKey(Scope.SCOPE_CELL, key.substring(15));
    }
    if (key.startsWith("tenantcfg:tenant:") && key.length() > 17) {
      return new TenantCfgKey(Scope.SCOPE_TENANT, key.substring(17));
    }
    return new Unknown(key);
  }
}
