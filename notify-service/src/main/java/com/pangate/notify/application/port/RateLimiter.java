package com.pangate.notify.application.port;

/** Counts per key in a one-minute window shared by every pod; when the counter is unavailable it allows. */
public interface RateLimiter {
  boolean tryAcquire(String key, int perMinute);
}
