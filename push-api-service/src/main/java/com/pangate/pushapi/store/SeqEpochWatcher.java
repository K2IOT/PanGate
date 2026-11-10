package com.pangate.pushapi.store;

import com.pangate.contracts.testkit.SeqKeys;
import com.pangate.pushapi.core.PushApiKeys;
import com.pangate.pushapi.core.SingleFlight;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.event.connection.ConnectionActivatedEvent;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bumps {@code seqepochs} for the slots of a Redis state shard whose master changed {@code run_id} (failover or a
 * restart that lost data; push-api spec §3.2, ADR 0024). The bump runs as one script on the shard that holds
 * {@code seqepochs} and {@code runids{seqepochs}}, so several pods seeing the same failover bump once. The pod also
 * remembers run_ids and epochs in memory: a standalone restart without persistence loses both hashes, and the new
 * epoch must still be above every epoch handed out before.
 */
public final class SeqEpochWatcher implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(SeqEpochWatcher.class);
  static final String BUMP = load("lua/seqepochs-bump.lua");
  static final String BUMP_SLOTS = """
      for i = 1, #ARGV do redis.call('HINCRBY', KEYS[1], ARGV[i], 1) end
      return #ARGV""";

  private final RedisState redis;
  private final RedisSeqEpochs epochs;
  private final Consumer<String> onBump;
  private final Map<String, String> known = new ConcurrentHashMap<>();
  private final SingleFlight flight = new SingleFlight();
  private ScheduledExecutorService timer;
  private reactor.core.Disposable events;

  public SeqEpochWatcher(RedisState redis, RedisSeqEpochs epochs, Consumer<String> onBump) {
    this.redis = redis;
    this.epochs = epochs;
    this.onBump = onBump;
  }

  private static String load(String resource) {
    try (InputStream in = SeqEpochWatcher.class.getClassLoader().getResourceAsStream(resource)) {
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * One pass over every shard that started after this call; callers arriving together (every request that saw a
   * seq regression after a failover) share it. Errors are logged and the pass is retried on the next tick.
   */
  public void checkNow() {
    checkNow("failover");
  }

  /** {@code reason} labels a bump this pass makes ({@code failover}, or {@code regression} from a seq regression). */
  public void checkNow(String reason) {
    flight.run(() -> pass(reason));
  }

  private void pass(String reason) {
    boolean bumped = false;
    boolean changed = false;
    try {
      java.util.List<RedisState.Shard> shards = redis.shards();
      for (RedisState.Shard s : shards) {
        String prev = known.getOrDefault(s.range(), "");
        Long r = redis.await(redis.async().<Long>eval(BUMP, ScriptOutputType.INTEGER,
            new String[] {PushApiKeys.RUNIDS, SeqKeys.SEQ_EPOCHS}, s.range(), s.runId(), Integer.toString(s.start()),
            Integer.toString(s.end()), prev, Integer.toString(epochs.maxEpoch(s.start(), s.end()))), "seqepochs bump");
        known.put(s.range(), s.runId());
        changed |= !prev.isEmpty() && !prev.equals(s.runId());   // another pod may have done the bump
        if (r != null && r == 1) {
          bumped = true;
          log.warn("Redis state shard {} changed run_id ({} → {}): seqepochs bumped", s.range(),
              prev.isEmpty() ? "?" : prev, s.runId());
        }
      }
      dropGoneRanges(shards);
    } catch (RuntimeException e) {
      log.warn("seqepochs check failed: {}", e.toString());
      return;
    }
    if (bumped || changed) {
      epochs.refreshNow();
    }
    if (bumped) {
      onBump.accept(reason);
    }
  }

  /** Spec §3.2: a range that no longer exists (resharding) loses its {@code runids} field. */
  private void dropGoneRanges(java.util.List<RedisState.Shard> shards) {
    Set<String> current = new java.util.HashSet<>();
    shards.forEach(s -> current.add(s.range()));
    String[] gone = redis.await(redis.async().hkeys(PushApiKeys.RUNIDS), "HKEYS runids").stream()
        .filter(f -> !current.contains(f)).toArray(String[]::new);
    if (gone.length > 0) {
      redis.await(redis.async().hdel(PushApiKeys.RUNIDS, gone), "HDEL runids");
      java.util.Arrays.stream(gone).forEach(known::remove);
    }
  }

  /** Admin: +1 on each slot; returns the number of slots bumped. */
  public int bump(Set<Integer> slots) {
    String[] args = slots.stream().filter(s -> s >= 0 && s < SeqKeys.SLOTS).map(String::valueOf)
        .toArray(String[]::new);
    if (args.length == 0) {
      return 0;
    }
    Long n = redis.await(redis.async().<Long>eval(BUMP_SLOTS, ScriptOutputType.INTEGER,
        new String[] {SeqKeys.SEQ_EPOCHS}, args), "seqepochs admin bump");
    epochs.refreshNow();
    onBump.accept("admin");
    return n == null ? 0 : n.intValue();
  }

  public int bumpAll() {
    Set<Integer> all = new java.util.HashSet<>();
    for (int s = 0; s < SeqKeys.SLOTS; s++) {
      all.add(s);
    }
    return bump(all);
  }

  /** Checks every {@code every}, right after Lettuce (re)connects, and when the cluster topology changes. */
  public synchronized void start(Duration every) {
    timer = Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().daemon().name("seqepochs").factory());
    timer.scheduleWithFixedDelay(this::checkNow, 0, every.toMillis(), TimeUnit.MILLISECONDS);
    events = redis.resources().eventBus().get().filter(e -> e instanceof ConnectionActivatedEvent
        || e instanceof io.lettuce.core.cluster.event.ClusterTopologyChangedEvent)
        .subscribe(e -> timer.execute(this::checkNow));
  }

  @Override
  public synchronized void close() {
    if (events != null) {
      events.dispose();
    }
    if (timer != null) {
      timer.shutdownNow();
    }
  }
}
