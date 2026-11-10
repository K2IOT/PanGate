package com.pangate.pushapi.store;

import com.pangate.contracts.testkit.SeqKeys;
import com.pangate.pushapi.core.SeqEpochs;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** {@code HGETALL seqepochs} cached as {@code int[16384]} (push-api spec §3.1); a failed refresh keeps the cache. */
public final class RedisSeqEpochs implements SeqEpochs {

  private static final Logger log = LoggerFactory.getLogger(RedisSeqEpochs.class);

  private final RedisState redis;
  private volatile int[] epochs = new int[SeqKeys.SLOTS];
  private volatile long refreshedAtMs;

  public RedisSeqEpochs(RedisState redis) {
    this.redis = redis;
  }

  @Override
  public int epoch(String principal) {
    return epochs[SeqKeys.slot(principal)];
  }

  @Override
  public void refreshNow() {
    try {
      Map<String, String> all = redis.await(redis.async().hgetall(SeqKeys.SEQ_EPOCHS), "HGETALL seqepochs");
      int[] next = new int[SeqKeys.SLOTS];
      all.forEach((slot, epoch) -> {
        try {
          int s = Integer.parseInt(slot);
          if (s >= 0 && s < SeqKeys.SLOTS) {
            next[s] = Integer.parseInt(epoch);
          }
        } catch (NumberFormatException ignored) {
          // not a slot field
        }
      });
      epochs = next;
      refreshedAtMs = System.currentTimeMillis();
    } catch (RuntimeException e) {
      log.warn("seqepochs not refreshed, keeping the cache: {}", e.toString());
    }
  }

  /** Highest cached epoch in slots {@code start..end} (the floor of a bump). */
  public int maxEpoch(int start, int end) {
    int[] e = epochs;
    int max = 0;
    for (int s = start; s <= end; s++) {
      max = Math.max(max, e[s]);
    }
    return max;
  }

  /** Wall time of the last successful refresh (metric {@code pushapi.seqepoch.cache_age_seconds}). */
  public long refreshedAtMs() {
    return refreshedAtMs;
  }
}
