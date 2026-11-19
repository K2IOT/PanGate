package com.pangate.chat.core;

import com.pangate.chat.config.ChatProperties;
import java.util.EnumMap;
import java.util.Map;
import java.util.function.LongSupplier;

/**
 * Perf 3a (spec §3.2): per-user limits of the HTTP API, one token bucket per user per group, per pod like every other
 * limit (spec §5). Checked after the request's own validation and before any store read; over the limit is
 * {@code 429 rate_limited}.
 */
public final class ApiLimits {

  /** The API groups of spec §3.2; MANAGE and OTHER are each one bucket shared by their endpoints. */
  public enum Bucket { LIST, STATE, SYNC, PRESENCE, HISTORY, MANAGE, OTHER }

  private final Map<Bucket, TokenBucketLimiter> buckets;

  public ApiLimits(ChatProperties.Api cfg, LongSupplier nanos) {
    buckets = new EnumMap<>(Bucket.class);
    buckets.put(Bucket.LIST, new TokenBucketLimiter(cfg.listRate(), cfg.listBurst(), nanos));
    buckets.put(Bucket.STATE, new TokenBucketLimiter(cfg.stateRate(), cfg.stateBurst(), nanos));
    buckets.put(Bucket.SYNC, new TokenBucketLimiter(cfg.syncRate(), cfg.syncBurst(), nanos));
    buckets.put(Bucket.PRESENCE, new TokenBucketLimiter(cfg.presenceRate(), cfg.presenceBurst(), nanos));
    buckets.put(Bucket.HISTORY, new TokenBucketLimiter(cfg.historyRate(), cfg.historyBurst(), nanos));
    buckets.put(Bucket.MANAGE, new TokenBucketLimiter(cfg.manageRate(), cfg.manageBurst(), nanos));
    buckets.put(Bucket.OTHER, new TokenBucketLimiter(cfg.otherRate(), cfg.otherBurst(), nanos));
  }

  private ApiLimits() {
    buckets = null;
  }

  /** No limit at all (tests that are not about limits). */
  public static ApiLimits unlimited() {
    return new ApiLimits();
  }

  public void check(Bucket b, String userId) {
    if (buckets != null && !buckets.get(b).tryAcquire(userId)) {
      throw new ChatRefusal(ChatRefusal.RATE_LIMITED, "too many requests, slow down");
    }
  }
}
