package com.pangate.gateway.control;

import com.pangate.contracts.v1.Scope;
import com.pangate.contracts.v1.TenantCfg;
import com.pangate.gateway.loop.LoopContext;
import com.pangate.gateway.loop.LoopRegistry;
import com.pangate.gateway.protocol.CloseCode;
import com.pangate.gateway.session.ConnRef;
import com.pangate.gateway.session.PrincipalIndex;
import com.pangate.gateway.session.Session;
import com.pangate.gateway.session.SessionState;
import java.util.ArrayList;
import java.util.List;
import java.util.function.LongSupplier;
import java.util.function.Predicate;

/**
 * Applies control events to sessions that are already open (spec §6.8, §7.8): revocations close with 4003, a blocked
 * tenant closes with 4013 (retry after 300 s), a tenant moved to another cell starts migrating. Runs after
 * {@link ControlState}; every session change is a task on its owning loop.
 */
public final class ControlEnforcer implements ControlEventHandler {

  static final long BLOCKED_RETRY_AFTER_MS = 300_000;

  private final PrincipalIndex principals;
  private final LoopRegistry loops;
  private final TenantLimitsStore tenants;
  private final CellMapView cells;
  private final TenantMigrator migrator;
  private final LongSupplier wallMillis;

  public ControlEnforcer(PrincipalIndex principals, LoopRegistry loops, TenantLimitsStore tenants, CellMapView cells,
      TenantMigrator migrator, LongSupplier wallMillis) {
    this.principals = principals;
    this.loops = loops;
    this.tenants = tenants;
    this.cells = cells;
    this.migrator = migrator;
    this.wallMillis = wallMillis;
  }

  @Override
  public void onRevoke(String key, long expiresAtMs) {
    if (expiresAtMs <= wallMillis.getAsLong()) {
      return;
    }
    if (key.startsWith("p:")) {
      String principal = key.substring(2);
      closePrincipal(principal, s -> true);
    } else if (key.startsWith("d:")) {
      int slash = key.lastIndexOf('/');
      String principal = key.substring(2, slash);
      String device = key.substring(slash + 1);
      closePrincipal(principal, s -> device.equals(s.identity().deviceId()));
    } else if (key.startsWith("j:")) {
      long hash = Long.parseLong(key.substring(2));
      closeEverywhere(s -> s.authJtiHash() == hash, CloseCode.REVOKED, "REVOKED", 0);
    }
  }

  private void closePrincipal(String principal, Predicate<Session> match) {
    for (ConnRef ref : principals.get(principal)) {
      LoopContext c = loops.get(ref.loopIdx());
      c.eventLoop().execute(() -> {
        Session s = c.session(ref.sessionKey());
        if (open(s) && principal.equals(s.identity().principalId()) && match.test(s)) {
          s.close(CloseCode.REVOKED, "REVOKED");
        }
      });
    }
  }

  @Override
  public void onTenantCfg(TenantCfg cfg) {
    boolean tenantScope = cfg.getScope() == Scope.SCOPE_TENANT;
    String tenant = cfg.getScopeId();
    if (tenantScope && tenants.policy(tenant).blocked()) {
      closeEverywhere(s -> tenant.equals(s.identity().tenantId()), CloseCode.OVERLOADED, "TENANT_BLOCKED",
          BLOCKED_RETRY_AFTER_MS);
    } else if (cfg.getSwitches().hasHeartbeatIntervalSec()) {
      // Timeout = 3x the new interval, effective at once (spec §7.8): move affected sessions to their new slot.
      Predicate<Session> affected = tenantScope ? s -> tenant.equals(s.identity().tenantId()) : s -> true;
      forEachOpen(affected, Session::refreshDeadline);
    }
  }

  @Override
  public void onTenantCfgRemoved(Scope scope, String id) {
    Predicate<Session> affected = scope == Scope.SCOPE_TENANT ? s -> id.equals(s.identity().tenantId()) : s -> true;
    forEachOpen(affected, Session::refreshDeadline);
  }

  @Override
  public void onCellMap(String tenant, String cell) {
    if (cells.servesTenant(tenant)) {
      migrator.stop(tenant);
    } else {
      migrator.start(tenant);
    }
  }

  @Override
  public void onCellMapRemoved(String tenant) {
    migrator.stop(tenant);
  }

  /** Revocations, blocks and config changes are rare: an O(n) scan per loop is acceptable (spec §6.8). */
  private void closeEverywhere(Predicate<Session> match, CloseCode code, String reason, long retryAfterMs) {
    forEachOpen(match, s -> s.close(code, reason, retryAfterMs));
  }

  /** Collects first, then acts: closing may unregister a session while the loop's map is being iterated. */
  private void forEachOpen(Predicate<Session> match, java.util.function.Consumer<Session> action) {
    for (LoopContext c : loops.all()) {
      c.eventLoop().execute(() -> {
        List<Session> hit = new ArrayList<>();
        c.forEachSession(s -> {
          if (open(s) && match.test(s)) {
            hit.add(s);
          }
        });
        hit.forEach(action);
      });
    }
  }

  private static boolean open(Session s) {
    return s != null && s.identity() != null
        && (s.state() == SessionState.ACTIVE || s.state() == SessionState.DRAINING);
  }
}
