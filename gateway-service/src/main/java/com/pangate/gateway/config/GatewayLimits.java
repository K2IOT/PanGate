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
    Duration sessionTtl,
    int inboundRate,
    int inboundBurst,
    int topicsPerConn,
    int pendingAckMsgs,
    int pendingAckBytes,
    Duration pendingAckTimeout,
    int broadcastRatePerLoop) {

  public static GatewayLimits from(GatewayProperties p) {
    GatewayProperties.Limits l = p.limits();
    // Deadlines run on a 1 s wheel: a shorter ack timeout would truncate to 0 and expire every push at the next tick.
    if (l.pendingAck().timeout().compareTo(Duration.ofSeconds(1)) < 0) {
      throw new IllegalArgumentException("gateway.limits.pending-ack.timeout must be at least 1s, got "
          + l.pendingAck().timeout());
    }
    return new GatewayLimits(l.hardCap(), l.softCap(), l.perIp(), l.frameMax(), l.helloMax(), l.helloTimeout(),
        l.handshakeTimeout(), l.handshakesPerPrincipalPerMin(), l.heartbeatInterval(), l.sessionTtl(), l.inboundRate(), l.inboundBurst(), l.topicsPerConn(),
        l.pendingAck().msgs(), (int) l.pendingAck().bytes().toBytes(), l.pendingAck().timeout(),
        p.broadcast().ratePerLoop());
  }

  /** Idle timeout = three missed heartbeats. */
  public long idleTimeoutSec() {
    return 3 * heartbeatInterval.toSeconds();
  }
}
