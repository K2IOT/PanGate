package com.pangate.notify.infrastructure.redis;

import com.pangate.notify.application.port.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code INCR notifyreg:<key>} with a 60 s expiry set on the first hit, or on any hit that finds none; Redis down lets
 * the request through.
 */
public final class RedisRateLimiter implements RateLimiter {

  private static final Logger log = LoggerFactory.getLogger(RedisRateLimiter.class);
  private final LazyRedis redis;

  public RedisRateLimiter(LazyRedis redis) {
    this.redis = redis;
  }

  @Override
  public boolean tryAcquire(String key, int perMinute) {
    try {
      String k = "notifyreg:" + key;
      var r = redis.sync();
      long n = r.incr(k);
      // review of PR #7: a counter left without a TTL (INCR went through, EXPIRE did not) gets one on the next hit,
      // instead of rate-limiting its device for good
      if (n == 1 || r.ttl(k) == -1) {
        r.expire(k, 60);
      }
      return n <= perMinute;
    } catch (RuntimeException e) {
      log.warn("registration limit not checked: {}", e.toString());
      return true;
    }
  }
}
