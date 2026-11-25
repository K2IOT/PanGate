package com.pangate.chat.kafka;

import com.pangate.chat.config.ChatProperties;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Perf 3c (spec §4.1): one Kafka consumer whose records run in lanes, one lane per (partition, record key), in offset
 * order within a lane and in parallel across lanes. Only the poll thread touches the consumer: it polls, queues each
 * record on its lane, pauses a partition holding {@code maxPendingPerPartition} unfinished records (resumes below half)
 * and commits, every {@code commitInterval}, each partition's watermark: its oldest unfinished offset. A failed batch
 * is retried in place (100 ms → 5 s), frozen; with {@code deadLetters}, a batch failing the same deterministic way
 * {@link PoisonGuard#MAX_ATTEMPTS} times gets a last try in which the handler dead-letters what fails. On revoke or
 * close, lanes stop taking batches, running batches get {@code revokeWait}, the watermark is committed, and lanes still
 * running are interrupted. A key's next batch never starts while its previous one still runs, across a rebalance too.
 */
public final class KeyedPipeline implements AutoCloseable {

  /** One batch of records of one key, in offset order. Throws to have the same batch retried. */
  @FunctionalInterface
  public interface Handler {
    void handle(List<ConsumerRecord<String, byte[]>> batch, boolean lastTry) throws Exception;
  }

  public record Settings(int maxPendingPerPartition, int maxBatch, int maxConcurrency, Duration commitInterval,
      Duration revokeWait, boolean deadLetters) {

    /** 3c minor: values that would never resume, spin or block are refused at start. */
    public Settings {
      if (maxPendingPerPartition < 2 || maxBatch < 1 || maxConcurrency < 1 || commitInterval == null
          || commitInterval.isZero() || commitInterval.isNegative() || revokeWait == null || revokeWait.isNegative()) {
        throw new IllegalArgumentException("chat.pipeline: max-pending-per-partition >= 2, max-batch >= 1, "
            + "max-concurrency >= 1, commit-interval > 0, revoke-wait >= 0; got " + maxPendingPerPartition + ", "
            + maxBatch + ", " + maxConcurrency + ", " + commitInterval + ", " + revokeWait);
      }
    }

    public static Settings of(ChatProperties.Pipeline p, int maxBatch, boolean deadLetters) {
      return new Settings(p.maxPendingPerPartition(), maxBatch, p.maxConcurrency(), p.commitInterval(),
          p.revokeWait(), deadLetters);
    }

    /** The {@code chat.pipeline.*} defaults (spec §4.6). */
    public static Settings defaults(int maxBatch, boolean deadLetters) {
      return new Settings(1_000, maxBatch, 256, Duration.ofMillis(200), Duration.ofSeconds(5), deadLetters);
    }
  }

  static final long MIN_BACKOFF_MS = 100;
  static final long MAX_BACKOFF_MS = 5_000;
  private static final Logger log = LoggerFactory.getLogger(KeyedPipeline.class);

  private final String name;
  private final Consumer<String, byte[]> consumer;
  private final String topic;
  private final Settings settings;
  private final Handler handler;
  private final Runnable onRevoked;
  private final Semaphore slots;
  private final Timer batchTimer;
  private final Counter transientRetries;
  private final Counter poisonRetries;
  /** Partitions assigned now; read by gauges. */
  private final Map<TopicPartition, Part> parts = new ConcurrentHashMap<>();
  /** The latest lane of each (partition, key), where new records of the key are queued. */
  private final Map<LaneId, Lane> lanes = new ConcurrentHashMap<>();
  /**
   * Final review C1: the lane holding each key, from before its first batch until it ends, across any number of
   * rebalances: a lane runs batches only while it holds its key, so two batches of one key never overlap.
   */
  private final Map<LaneId, Lane> holders = new ConcurrentHashMap<>();
  /** Final review: retry warnings at most once a second per pipeline; the counters carry the volume. */
  private final java.util.concurrent.atomic.AtomicLong nextWarnNanos = new java.util.concurrent.atomic.AtomicLong(System.nanoTime());
  private final java.util.concurrent.atomic.AtomicLong unloggedFailures = new java.util.concurrent.atomic.AtomicLong();
  private final ConcurrentLinkedQueue<Done> done = new ConcurrentLinkedQueue<>();
  private volatile boolean running = true;
  private volatile int pausedCount;
  private volatile long lag;
  private long nextCommitNanos;
  private Thread thread;

  private record LaneId(TopicPartition tp, String key) {}

  private record Done(Part part, List<Long> offsets) {}

  /** One assignment of a partition. */
  private static final class Part {
    final TopicPartition tp;
    /** Unfinished offsets → when they were received (nanos). */
    final ConcurrentSkipListMap<Long, Long> pending = new ConcurrentSkipListMap<>();
    final CountDownLatch gone = new CountDownLatch(1);
    long next = -1;
    long committed = -1;
    boolean paused;

    Part(TopicPartition tp) {
      this.tp = tp;
    }

    boolean gone() {
      return gone.getCount() == 0;
    }

    long watermark() {
      return pending.isEmpty() ? next : pending.firstKey();
    }
  }

  private final class Lane implements Runnable {
    final LaneId id;
    final Part part;
    final ArrayDeque<ConsumerRecord<String, byte[]>> queue = new ArrayDeque<>();
    final CountDownLatch finished = new CountDownLatch(1);
    boolean ending;
    volatile Thread worker;

    Lane(LaneId id, Part part) {
      this.id = id;
      this.part = part;
    }

    synchronized boolean offer(ConsumerRecord<String, byte[]> r) {
      if (ending) {
        return false;
      }
      queue.add(r);
      return true;
    }

    private synchronized List<ConsumerRecord<String, byte[]>> take() {
      if (queue.isEmpty() || part.gone() || !running) {
        ending = true;
        return null;
      }
      List<ConsumerRecord<String, byte[]>> batch = new ArrayList<>();
      while (!queue.isEmpty() && batch.size() < settings.maxBatch()) {
        batch.add(queue.poll());
      }
      return batch;
    }

    @Override
    public void run() {
      worker = Thread.currentThread();
      boolean held = false;
      try {
        Lane holder;
        while ((holder = holders.putIfAbsent(id, this)) != null) {
          holder.finished.await(50, TimeUnit.MILLISECONDS);
          if (part.gone() || !running) {
            synchronized (this) {
              ending = true;
            }
            return;
          }
        }
        held = true;
        List<ConsumerRecord<String, byte[]>> batch;
        while ((batch = take()) != null) {
          if (!handleUntilDone(batch)) {
            synchronized (this) {
              ending = true;
            }
            return;
          }
          done.add(new Done(part, batch.stream().map(ConsumerRecord::offset).toList()));
        }
      } catch (InterruptedException e) {
        synchronized (this) {
          ending = true;
        }
      } finally {
        lanes.remove(id, this);
        if (held) {
          holders.remove(id, this);
        }
        finished.countDown();
      }
    }

    /** True once the handler took the batch; false when the partition went away or the pipeline closed first. */
    private boolean handleUntilDone(List<ConsumerRecord<String, byte[]>> batch) throws InterruptedException {
      int failures = 0;
      long backoff = MIN_BACKOFF_MS;
      while (true) {
        if (part.gone() || !running) {
          return false;
        }
        boolean lastTry = settings.deadLetters() && failures >= PoisonGuard.MAX_ATTEMPTS - 1;
        slots.acquire();
        if (part.gone() || !running) {   // 3c minor: the partition went while this lane waited for a slot
          slots.release();
          return false;
        }
        long t0 = System.nanoTime();
        try {
          handler.handle(batch, lastTry);
          return true;
        } catch (InterruptedException e) {
          throw e;
        } catch (Throwable e) {
          // final review I1: an Error too (a bug, a class that failed to load) is retried, never skipped
          if (PoisonGuard.isTransient(e)) {
            transientRetries.increment();
            failures = 0;   // 3c minor: "in a row" (spec §4.1)
          } else {
            failures++;
            poisonRetries.increment();
          }
          warn(batch, e);
        } finally {
          batchTimer.record(System.nanoTime() - t0, TimeUnit.NANOSECONDS);
          slots.release();
        }
        if (part.gone.await(backoff, TimeUnit.MILLISECONDS)) {
          return false;
        }
        backoff = Math.min(MAX_BACKOFF_MS, backoff * 2);
      }
    }
  }

  private void warn(List<ConsumerRecord<String, byte[]>> batch, Throwable e) {
    long now = System.nanoTime();
    long next = nextWarnNanos.get();
    if (now - next < 0 || !nextWarnNanos.compareAndSet(next, now + TimeUnit.SECONDS.toNanos(1))) {
      unloggedFailures.incrementAndGet();
      return;
    }
    long more = unloggedFailures.getAndSet(0);
    ConsumerRecord<String, byte[]> r = batch.getFirst();
    log.warn("{}: batch at {}-{}@{} failed: {}; retrying ({} more failures since the last warning)", name,
        r.topic(), r.partition(), r.offset(), e.toString(), more);
  }

  public KeyedPipeline(String name, Consumer<String, byte[]> consumer, String topic, Settings settings,
      Handler handler, Runnable onRevoked, MeterRegistry meters) {
    this.name = name;
    this.consumer = consumer;
    this.topic = topic;
    this.settings = settings;
    this.handler = handler;
    this.onRevoked = onRevoked;
    this.slots = new Semaphore(settings.maxConcurrency());
    this.batchTimer = meters.timer("chat.pipeline.batch", "pipeline", name);
    this.transientRetries = meters.counter("chat.pipeline.retries", "pipeline", name, "reason", "transient");
    this.poisonRetries = meters.counter("chat.pipeline.retries", "pipeline", name, "reason", "poison");
    Gauge.builder("chat.pipeline.pending", parts, ps -> ps.values().stream().mapToInt(p -> p.pending.size()).sum())
        .tag("pipeline", name).register(meters);
    // 3c minor: lanes with work are those holding their key, a straggler of an old assignment included
    Gauge.builder("chat.pipeline.lanes", holders, Map::size).tag("pipeline", name).register(meters);
    Gauge.builder("chat.pipeline.paused", this, x -> x.pausedCount).tag("pipeline", name).register(meters);
    Gauge.builder("chat.pipeline.oldest", this, KeyedPipeline::oldestMs).tag("pipeline", name).register(meters);
    Gauge.builder("chat.kafka.lag", this, x -> x.lag).tag("pipeline", name).register(meters);
  }

  private double oldestMs() {
    long now = System.nanoTime();
    long oldest = now;
    for (Part p : parts.values()) {
      Map.Entry<Long, Long> e = p.pending.firstEntry();
      if (e != null) {
        oldest = Math.min(oldest, e.getValue());
      }
    }
    return (now - oldest) / 1e6;
  }

  public synchronized void start() {
    thread = Thread.ofPlatform().daemon().name("chat-" + name).start(this::run);
  }

  private void run() {
    try {
      consumer.subscribe(List.of(topic), new ConsumerRebalanceListener() {
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> revoked) {
          release(revoked);
          onRevoked.run();
        }

        @Override
        public void onPartitionsLost(Collection<TopicPartition> lost) {
          release(lost);
          onRevoked.run();
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> assigned) {
          for (TopicPartition tp : assigned) {
            parts.putIfAbsent(tp, new Part(tp));
          }
        }
      });
      while (running) {
        try {
          step();
        } catch (WakeupException e) {
          throw e;
        } catch (RuntimeException e) {
          log.warn("{}: {}; going on", name, e.toString());
          Thread.sleep(1_000);
        }
      }
    } catch (WakeupException e) {
      // closing
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      try {
        release(List.copyOf(parts.keySet()));
      } catch (RuntimeException e) {
        log.warn("{}: final commit failed: {}", name, e.toString());
      }
      consumer.close(Duration.ofSeconds(2));
    }
  }

  private void step() {
    applyDone();
    for (ConsumerRecord<String, byte[]> r : consumer.poll(Duration.ofMillis(100))) {
      TopicPartition tp = new TopicPartition(r.topic(), r.partition());
      Part p = parts.computeIfAbsent(tp, Part::new);
      p.pending.put(r.offset(), System.nanoTime());
      p.next = r.offset() + 1;
      dispatch(p, r);
    }
    applyDone();
    int paused = 0;
    for (Part p : parts.values()) {
      int n = p.pending.size();
      if (!p.paused && n >= settings.maxPendingPerPartition()) {
        consumer.pause(List.of(p.tp));
        p.paused = true;
      } else if (p.paused && n < settings.maxPendingPerPartition() / 2) {
        consumer.resume(List.of(p.tp));
        p.paused = false;
      }
      if (p.paused) {
        paused++;
      }
    }
    pausedCount = paused;
    if (System.nanoTime() >= nextCommitNanos) {
      nextCommitNanos = System.nanoTime() + settings.commitInterval().toNanos();
      Map<TopicPartition, OffsetAndMetadata> out = watermarks(parts.values());
      if (!out.isEmpty()) {
        consumer.commitAsync(out, (offsets, e) -> {
          if (e != null) {
            log.warn("{}: commit failed: {}", name, e.toString());
            offsets.keySet().forEach(tp -> {
              Part p = parts.get(tp);
              if (p != null) {
                p.committed = -1;   // sent again next time
              }
            });
          }
        });
      }
      updateLag();
    }
  }

  private void dispatch(Part p, ConsumerRecord<String, byte[]> r) {
    LaneId id = new LaneId(p.tp, r.key());
    Lane current = lanes.get(id);
    if (current != null && current.part == p && current.offer(r)) {
      return;
    }
    Lane lane = new Lane(id, p);
    lane.offer(r);
    lanes.put(id, lane);
    Thread.ofVirtual().name("chat-" + name + "-lane").start(lane);
  }

  private void applyDone() {
    Done d;
    while ((d = done.poll()) != null) {
      for (long o : d.offsets()) {
        d.part().pending.remove(o);
      }
    }
  }

  /** Watermarks that moved since they were last committed; marks them committed. */
  private Map<TopicPartition, OffsetAndMetadata> watermarks(Collection<Part> of) {
    Map<TopicPartition, OffsetAndMetadata> out = new HashMap<>();
    for (Part p : of) {
      long w = p.watermark();
      if (w >= 0 && w != p.committed) {
        out.put(p.tp, new OffsetAndMetadata(w));
        p.committed = w;
      }
    }
    return out;
  }

  private void updateLag() {
    long sum = 0;
    for (TopicPartition tp : parts.keySet()) {
      try {
        OptionalLong l = consumer.currentLag(tp);
        if (l.isPresent()) {
          sum += l.getAsLong();
        }
      } catch (RuntimeException e) {
        // not known yet
      }
    }
    lag = sum;
  }

  /**
   * Partitions going away (revoked, lost, or closing): no new batches, running ones get {@code revokeWait}, the
   * watermark is committed, lanes still running are interrupted. Runs on the poll thread.
   */
  private void release(Collection<TopicPartition> tps) {
    List<Part> going = new ArrayList<>();
    for (TopicPartition tp : tps) {
      Part p = parts.remove(tp);
      if (p != null) {
        p.gone.countDown();
        going.add(p);
      }
    }
    if (going.isEmpty()) {
      return;
    }
    long deadline = System.nanoTime() + settings.revokeWait().toNanos();
    List<Lane> stragglers = new ArrayList<>();
    for (Lane l : List.copyOf(lanes.values())) {
      if (!going.contains(l.part)) {
        continue;
      }
      try {
        if (!l.finished.await(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS)) {
          stragglers.add(l);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        stragglers.add(l);
      }
    }
    applyDone();
    Map<TopicPartition, OffsetAndMetadata> out = watermarks(going);
    if (!out.isEmpty()) {
      try {
        try {
          consumer.commitSync(out);
        } catch (WakeupException e) {
          consumer.commitSync(out);   // 3c minor: a wakeup still pending from close(); it is cleared once thrown
        }
      } catch (RuntimeException e) {
        log.warn("{}: commit of released partitions failed: {}", name, e.toString());
      }
    }
    for (Lane l : stragglers) {
      Thread w = l.worker;
      if (w != null) {
        w.interrupt();
      }
    }
    pausedCount = (int) parts.values().stream().filter(p -> p.paused).count();
  }

  /** True while the poll thread still runs (tests; close() returns once it is false). */
  boolean pollThreadAlive() {
    Thread t = thread;
    return t != null && t.isAlive();
  }

  @Override
  public synchronized void close() {
    running = false;
    consumer.wakeup();
    if (thread != null) {
      try {
        // 3c minor: a revoke in progress and the final release may each take revoke-wait
        thread.join(2 * settings.revokeWait().toMillis() + 5_000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
