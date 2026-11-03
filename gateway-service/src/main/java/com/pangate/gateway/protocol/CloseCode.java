package com.pangate.gateway.protocol;

/** Close codes of protocol pangate.v1 (spec §4.8). ABNORMAL is internal bookkeeping and never sent. */
public enum CloseCode {
  NORMAL(1000, false, 0),
  GOING_AWAY(1001, true, 0),
  ABNORMAL(1006, true, 0),
  BAD_FRAME(4000, true, 0),
  AUTH_FAILED(4001, false, 0),
  SESSION_EXPIRED(4002, false, 0),
  REVOKED(4003, false, 0),
  HELLO_TIMEOUT(4004, true, 0),
  SLOW_CONSUMER(4008, true, 0),
  RATE_LIMITED(4009, true, 30_000),
  UPGRADE_REQUIRED(4010, false, 0),
  WRONG_CELL(4011, true, 0),
  DUPLICATE_SESSION(4012, false, 0),
  OVERLOADED(4013, true, 15_000),
  INTERNAL(4500, true, 0);

  private final int code;
  private final boolean retryable;
  private final long defaultRetryAfterMs;

  CloseCode(int code, boolean retryable, long defaultRetryAfterMs) {
    this.code = code;
    this.retryable = retryable;
    this.defaultRetryAfterMs = defaultRetryAfterMs;
  }

  public int code() {
    return code;
  }

  public boolean retryable() {
    return retryable;
  }

  public long defaultRetryAfterMs() {
    return defaultRetryAfterMs;
  }

  public static CloseCode of(int code) {
    for (CloseCode c : values()) {
      if (c.code == code) {
        return c;
      }
    }
    return code == 1000 ? NORMAL : ABNORMAL;
  }
}
