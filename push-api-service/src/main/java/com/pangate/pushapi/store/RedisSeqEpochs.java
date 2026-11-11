package com.pangate.pushapi.store;

import com.pangate.contracts.testkit.SeqKeys;
import com.pangate.pushapi.core.SeqEpochs;
import com.pangate.pushapi.core.SingleFlight;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code HGETALL seqepochs} cached as {@code int[16384]} (push-api spec §3.1); a failed refresh keeps the cache. An
 * epoch never goes down in the cache: a hash lost with the data (restart without persistence) reads as zeros until the
 * watcher bumps it, and the cache is the floor of that bump. Concurrent refreshes share one {@code HGETALL}.
 */
public final class RedisSeqEpochs implements SeqEpochs {

  private static final Logger log = LoggerFactory.getLogger(RedisSeqEpochs.class);

  private final RedisState redis;
  private volatile int[] epochs = new int[SeqKeys.SLOTS];
  private volatile long refreshedAtMs;
  private final SingleFlight flight = new SingleFlight();

  public RedisSeqEpochs(RedisState redis) {
    this.redis = redis;
  }

  @Override
  public int epoch(String principal) {
    return epochs[SeqKeys.epochSlot(principal)];
  }

  @Override
  public void refreshNow() {
    flight.run(this::refresh);
  }

  private void refresh() {
    try {
      Map<String, String> all = redis.await(redis.async().hgetall(SeqKeys.SEQ_EPOCHS), "HGETALL seqepochs");
      int[] next = epochs.clone();
      all.forEach((slot, epoch) -> {
        try {
          int s = Integer.parseInt(slot);
          if (s >= 0 && s < SeqKeys.SLOTS) {
            next[s] = Math.max(next[s], Integer.parseInt(epoch));
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

  /** Cached epoch of one slot (admin response). */
  public int epochAt(int slot) {
    return epochs[slot];
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
