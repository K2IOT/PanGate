package com.pangate.gateway.registry;

import com.pangate.gateway.lifecycle.RedisConnections;
import io.lettuce.core.cluster.SlotHash;
import java.time.Duration;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code GET seq:{p}} on the state Redis with a 200 ms budget, and the {@code seqepochs} hash (slot → epoch, written
 * by Push API) refreshed every 5 s into an {@code int[16384]} indexed by the Redis cluster slot of the principal.
 */
public final class RedisSeqReader implements SeqReader {

  private static final Logger log = LoggerFactory.getLogger(RedisSeqReader.class);
  private static final int SLOTS = 16_384;

  private final RedisConnections redis;
  private final ScheduledExecutorService tick;
  private final Duration timeout;
  private final AtomicReference<int[]> epochs = new AtomicReference<>(new int[SLOTS]);

  public RedisSeqReader(RedisConnections redis, ScheduledExecutorService tick, Duration timeout) {
    this.redis = redis;
    this.tick = tick;
    this.timeout = timeout;
  }

  @Override
  public CompletionStage<OptionalLong> read(String principal) {
    CompletableFuture<String> get;
    try {
      get = redis.state().get(RegistryKeys.seq(principal)).toCompletableFuture();
    } catch (RuntimeException e) {
      return CompletableFuture.completedFuture(OptionalLong.empty());
    }
    return get.orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS).handle((v, e) -> {
      if (e != null) {
        return OptionalLong.empty();
      }
      if (v == null) {
        return OptionalLong.of(0);
      }
      try {
        return OptionalLong.of(Long.parseLong(v));
      } catch (NumberFormatException nfe) {
        return OptionalLong.empty();
      }
    });
  }

  @Override
  public int seqEpoch(String principal) {
    return epochs.get()[SlotHash.getSlot(principal)];
  }

  /** Async; keeps the previous table on error. */
  public void refreshEpochs() {
    try {
      redis.state().hgetall(RegistryKeys.SEQ_EPOCHS).whenComplete((m, e) -> {
        if (e != null) {
          log.debug("seqepochs refresh failed: {}", e.toString());
        } else {
          epochs.set(table(m));
        }
      });
    } catch (RuntimeException e) {
      log.debug("seqepochs refresh failed: {}", e.toString());
    }
  }

  private static int[] table(Map<String, String> m) {
    int[] t = new int[SLOTS];
    m.forEach((slot, epoch) -> {
      try {
        int s = Integer.parseInt(slot);
        if (s >= 0 && s < SLOTS) {
          t[s] = Integer.parseInt(epoch);
        }
      } catch (NumberFormatException ignored) {
        // not a slot entry
      }
    });
    return t;
  }

  public void start() {
    tick.scheduleAtFixedRate(this::refreshEpochs, 5, 5, TimeUnit.SECONDS);
  }
}
