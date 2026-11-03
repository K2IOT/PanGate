package com.pangate.gateway.control;

import com.pangate.contracts.v1.KillSwitches;
import com.pangate.contracts.v1.Scope;
import com.pangate.contracts.v1.TenantCfg;
import com.pangate.contracts.v1.TenantLimits;
import com.pangate.gateway.config.GatewayLimits;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * TenantCfg from gw.control merged into effective {@link TenantPolicy} snapshots (spec §6.8). Reads are one volatile
 * read plus a map lookup; every update rebuilds the snapshot (copy-on-write; TenantCfg is rare). Only this cell's
 * CELL scope applies.
 */
public final class TenantLimitsStore {

  private record Snapshot(TenantPolicy base, Map<String, TenantPolicy> tenants) {}

  private final String ownCell;
  private final Supplier<GatewayLimits> limits;
  private final Object lock = new Object();
  private TenantCfg fleet;
  private TenantCfg cell;
  private final Map<String, TenantCfg> tenantCfgs = new HashMap<>();
  private volatile Snapshot snapshot;

  public TenantLimitsStore(String ownCell, Supplier<GatewayLimits> limits) {
    this.ownCell = ownCell;
    this.limits = limits;
    rebuild();
  }

  public TenantPolicy policy(String tenantId) {
    Snapshot s = snapshot;
    TenantPolicy p = tenantId == null ? null : s.tenants.get(tenantId);
    return p != null ? p : s.base;
  }

  public void apply(TenantCfg cfg) {
    synchronized (lock) {
      switch (cfg.getScope()) {
        case SCOPE_FLEET -> fleet = cfg;
        case SCOPE_CELL -> {
          if (!ownCell.equals(cfg.getScopeId())) {
            return;
          }
          cell = cfg;
        }
        case SCOPE_TENANT -> {
          if (cfg.getScopeId().isEmpty()) {
            return;
          }
          tenantCfgs.put(cfg.getScopeId(), cfg);
        }
        default -> {
          return;
        }
      }
      rebuild();
    }
  }

  public void remove(Scope scope, String id) {
    synchronized (lock) {
      switch (scope) {
        case SCOPE_FLEET -> fleet = null;
        case SCOPE_CELL -> {
          if (ownCell.equals(id)) {
            cell = null;
          }
        }
        case SCOPE_TENANT -> tenantCfgs.remove(id);
        default -> {
          return;
        }
      }
      rebuild();
    }
  }

  private void rebuild() {
    TenantPolicy base = merge(merge(TenantPolicy.defaults(limits.get()), fleet), cell);
    Map<String, TenantPolicy> tenants = new HashMap<>();
    tenantCfgs.forEach((t, cfg) -> tenants.put(t, merge(base, cfg)));
    snapshot = new Snapshot(base, Map.copyOf(tenants));
  }

  static TenantPolicy merge(TenantPolicy p, TenantCfg cfg) {
    if (cfg == null) {
      return p;
    }
    TenantLimits l = cfg.getLimits();
    KillSwitches k = cfg.getSwitches();
    Set<String> origins = l.getOriginAllowlistCount() > 0
        ? new LinkedHashSet<>(l.getOriginAllowlistList()) : p.originAllowlist();
    return new TenantPolicy(
        k.hasHeartbeatIntervalSec() && k.getHeartbeatIntervalSec() > 0 ? k.getHeartbeatIntervalSec() : p.heartbeatSec(),
        l.hasInboundRate() && l.getInboundRate() > 0 ? l.getInboundRate() : p.inboundRate(),
        l.hasInboundBurst() && l.getInboundBurst() > 0 ? l.getInboundBurst() : p.inboundBurst(),
        l.hasTopicsPerConn() ? l.getTopicsPerConn() : p.topicsPerConn(),
        l.hasMaxConns() ? l.getMaxConns() : p.maxConns(),
        l.hasMinSdkVersion() ? l.getMinSdkVersion() : p.minSdkVersion(),
        origins,
        l.hasSingleSession() ? l.getSingleSession() : p.singleSession(),
        k.hasTenantBlocked() ? k.getTenantBlocked() : p.blocked(),
        k.hasUpstreamTelemetryEnabled() ? k.getUpstreamTelemetryEnabled() : p.telemetryEnabled(),
        k.hasBroadcastEnabled() ? k.getBroadcastEnabled() : p.broadcastEnabled(),
        k.hasDeliveryDowngradeReliable() ? k.getDeliveryDowngradeReliable() : p.downgradeReliable(),
        k.hasRecycleEnabled() ? k.getRecycleEnabled() : p.recycleEnabled(),
        k.hasSseFallbackEnabled() ? k.getSseFallbackEnabled() : p.sseFallbackEnabled(),
        k.hasAdmissionRatePerNode() ? k.getAdmissionRatePerNode() : p.admissionRatePerNode());
  }
}
