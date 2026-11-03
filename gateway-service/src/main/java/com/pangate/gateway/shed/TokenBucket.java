package com.pangate.gateway.shed;

/** Classic token bucket on a nanosecond clock. Not thread-safe: one bucket per event loop. */
public final class TokenBucket {

  private double ratePerNano;
  private double burst;
  private double tokens;
  private long lastNanos;

  public TokenBucket(double ratePerSec, double burst, long nowNanos) {
    this.ratePerNano = ratePerSec / 1e9;
    this.burst = burst;
    this.tokens = burst;
    this.lastNanos = nowNanos;
  }

  public boolean tryAcquire(long nowNanos) {
    long elapsed = nowNanos - lastNanos;
    if (elapsed > 0) {
      tokens = Math.min(burst, tokens + elapsed * ratePerNano);
      lastNanos = nowNanos;
    }
    if (tokens >= 1.0) {
      tokens -= 1.0;
      return true;
    }
    return false;
  }

  public void setRate(double ratePerSec, double burst) {
    this.ratePerNano = ratePerSec / 1e9;
    this.burst = burst;
    this.tokens = Math.min(this.tokens, burst);
  }
}
