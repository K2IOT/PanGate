package com.pangate.gateway.config;

import java.time.Duration;

/** Immutable snapshot of the per-connection limits read on the hot path; swapped as a whole via {@link LimitsHolder}. */
public record GatewayLimits(
    int hardCap,
    int softCap,
    int perIp,
    int frameMax,
    int helloMax,
    Duration helloTimeout,
    Duration handshakeTimeout,
    int handshakesPerPrincipalPerMin,
    Duration heartbeatInterval,
    Duration sessionTtl) {

  public static GatewayLimits from(GatewayProperties p) {
    GatewayProperties.Limits l = p.limits();
    return new GatewayLimits(l.hardCap(), l.softCap(), l.perIp(), l.frameMax(), l.helloMax(), l.helloTimeout(),
        l.handshakeTimeout(), l.handshakesPerPrincipalPerMin(), l.heartbeatInterval(), l.sessionTtl());
  }

  /** Idle timeout = three missed heartbeats. */
  public long idleTimeoutSec() {
    return 3 * heartbeatInterval.toSeconds();
  }
}
