package com.pangate.account.domain;

/** 429 with Retry-After (account spec §7): {@code rate_limited} or {@code too_many_attempts}. */
public final class RateLimitedException extends DomainException {

  private final long retryAfterSec;

  public RateLimitedException(String code, long retryAfterSec) {
    super(code, "too many requests, try again later");
    this.retryAfterSec = Math.max(1, retryAfterSec);
  }

  public long retryAfterSec() {
    return retryAfterSec;
  }
}
