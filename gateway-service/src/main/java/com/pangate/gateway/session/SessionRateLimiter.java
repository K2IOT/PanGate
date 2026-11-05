package com.pangate.gateway.session;

import com.pangate.gateway.control.TenantPolicy;

/**
 * Per-session inbound limit for UPSTREAM (spec §5.5 step 1): token bucket of {@code inboundRate}/s, burst
 * {@code inboundBurst}; three consecutive seconds with a violation close the session (4009). Owner loop only.
 */
public final class SessionRateLimiter {

  public enum Verdict { OK, LIMITED, CLOSE }

  private SessionRateLimiter() {}

  public static Verdict check(Session s, TenantPolicy p, long nowNanos, long nowSec) {
    if (s.tokens < 0) {
      s.tokens = p.inboundBurst();
      s.lastRefillNanos = nowNanos;
    } else if (nowNanos > s.lastRefillNanos) {
      s.tokens = (float) Math.min(p.inboundBurst(),
          s.tokens + (nowNanos - s.lastRefillNanos) * (double) p.inboundRate() / 1_000_000_000d);
      s.lastRefillNanos = nowNanos;
    }
    if (s.tokens >= 1) {
      s.tokens -= 1;
      return Verdict.OK;
    }
    if (s.lastStrikeSec != nowSec) {
      s.strikes = s.lastStrikeSec == nowSec - 1 ? s.strikes + 1 : 1;
      s.lastStrikeSec = nowSec;
    }
    return s.strikes >= 3 ? Verdict.CLOSE : Verdict.LIMITED;
  }
}
