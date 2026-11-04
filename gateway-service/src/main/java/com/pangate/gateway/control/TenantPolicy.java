package com.pangate.gateway.control;

import com.pangate.gateway.config.GatewayLimits;
import java.util.Set;

/**
 * Effective per-tenant limits and kill switches (spec §7.8, §9): defaults ← fleet ← cell ← tenant. Kill switches are
 * fields here (spec §3.1's KillSwitches folded in). maxConns 0 = unlimited; admissionRatePerNode 0 = not set.
 */
public record TenantPolicy(
    int heartbeatSec,
    int inboundRate,
    int inboundBurst,
    int topicsPerConn,
    int maxConns,
    String minSdkVersion,
    Set<String> originAllowlist,
    boolean singleSession,
    boolean blocked,
    boolean telemetryEnabled,
    boolean broadcastEnabled,
    boolean downgradeReliable,
    boolean recycleEnabled,
    boolean sseFallbackEnabled,
    int admissionRatePerNode) {

  public TenantPolicy {
    originAllowlist = Set.copyOf(originAllowlist);
  }

  public long idleTimeoutSec() {
    return 3L * heartbeatSec;
  }

  public static TenantPolicy defaults(GatewayLimits l) {
    return new TenantPolicy((int) l.heartbeatInterval().toSeconds(), l.inboundRate(), l.inboundBurst(),
        l.topicsPerConn(), 0, "", Set.of(), false, false, true, true, false, true, false, 0);
  }
}
