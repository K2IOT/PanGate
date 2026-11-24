package com.pangate.chat.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.pangate.chat.core.StoreUnavailableException;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Perf 3c (spec §4.1): lanes per key, a commit watermark, pause, in-place retry, revoke and close. */
class KeyedPipelineTest {

  static final String T = "t";
  final TopicPartition p0 = new TopicPartition(T, 0);
  final TopicPartition p1 = new TopicPartition(T, 1);
  final SimpleMeterRegistry meters = new SimpleMeterRegistry();
  final AtomicInteger revoked = new AtomicInteger();
  final TestConsumer c = new TestConsumer();
  KeyedPipeline pipeline;

  /** Records commits (MockConsumer forgets them once closed), seeks and polls. */
  static final class TestConsumer extends MockConsumer<String, byte[]> {
    final Map<TopicPartition, Long> commits = new ConcurrentHashMap<>();
    final AtomicInteger seeks = new AtomicInteger();
    final AtomicInteger polls = new AtomicInteger();

    TestConsumer() {
      super(OffsetResetStrategy.EARLIEST);
    }

    @Override
    public synchronized ConsumerRecords<String, byte[]> poll(Duration timeout) {
      polls.incrementAndGet();
      return super.poll(timeout);
    }

    @Override
    public synchronized void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
      offsets.forEach((tp, o) -> commits.put(tp, o.offset()));
      super.commitSync(offsets);
    }

    @Override
    public synchronized void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback cb) {
      offsets.forEach((tp, o) -> commits.put(tp, o.offset()));
      super.commitAsync(offsets, cb);
    }

    @Override
    public synchronized void seek(TopicPartition tp, long offset) {
      seeks.incrementAndGet();
      super.seek(tp, offset);
    }
  }

  static KeyedPipeline.Settings settings(int maxPending, int maxBatch, int maxConcurrency, boolean deadLetters) {
    return new KeyedPipeline.Settings(maxPending, maxBatch, maxConcurrency, Duration.ofMillis(50),
        Duration.ofSeconds(1), deadLetters);
  }

  void start(KeyedPipeline.Settings s, KeyedPipeline.Handler h) {
    pipeline = new KeyedPipeline("test", c, T, s, h, revoked::incrementAndGet, meters);
    c.schedulePollTask(() -> {
      c.rebalance(List.of(p0, p1));
      c.updateBeginningOffsets(new HashMap<>(Map.of(p0, 0L, p1, 0L)));
    });
    pipeline.start();
    await().until(() -> c.assignment().size() == 2);
  }

  void add(TopicPartition tp, long offset, String key) {
    c.addRecord(new ConsumerRecord<>(T, tp.partition(), offset, key, new byte[0]));
  }

  static List<Long> offsets(List<ConsumerRecord<String, byte[]>> batch) {
    return batch.stream().map(ConsumerRecord::offset).toList();
  }

  @AfterEach
  void stop() {
    if (pipeline != null) {
      pipeline.close();
    }
  }

  @Test
  void orderIsKeptWithinAKey() {
    List<Long> seen = Collections.synchronizedList(new ArrayList<>());
    AtomicInteger inFlight = new AtomicInteger();
    AtomicInteger most = new AtomicInteger();
    start(settings(100, 1, 16, true), (batch, last) -> {
      most.accumulateAndGet(inFlight.incrementAndGet(), Math::max);
      Thread.sleep(1);
      seen.addAll(offsets(batch));
      inFlight.decrementAndGet();
    });
    for (int i = 0; i < 50; i++) {
      add(p0, i, "a");
    }
    await().until(() -> seen.size() == 50);
    assertThat(seen).isSorted();
    assertThat(most.get()).isEqualTo(1);
  }

  @Test
  void keysOfOnePartitionRunInParallel() {
    CountDownLatch release = new CountDownLatch(1);
    List<String> done = Collections.synchronizedList(new ArrayList<>());
    start(settings(100, 1, 16, true), (batch, last) -> {
      if ("a".equals(batch.getFirst().key())) {
        release.await();
      }
      done.add(batch.getFirst().key());
    });
    add(p0, 0, "a");
    add(p0, 1, "b");
    await().until(() -> done.contains("b"));
    assertThat(done).doesNotContain("a");
    release.countDown();
    await().until(() -> done.contains("a"));
  }

  @Test
  void commitNeverPassesAnUnfinishedRecord() throws Exception {
    CountDownLatch release = new CountDownLatch(1);
    AtomicInteger done = new AtomicInteger();
    start(settings(100, 1, 16, true), (batch, last) -> {
      if ("a".equals(batch.getFirst().key())) {
        release.await();
      }
      done.incrementAndGet();
    });
    add(p0, 0, "a");
    add(p0, 1, "b");
    add(p0, 2, "b");
    add(p0, 3, "c");
    await().until(() -> done.get() == 3);
    Thread.sleep(300);
    assertThat(c.commits.getOrDefault(p0, 0L)).isZero();
    release.countDown();
    await().until(() -> c.commits.getOrDefault(p0, 0L) == 4L);
  }

  @Test
  void pausesAtMaxPendingAndResumesBelowHalf() throws Exception {
    CountDownLatch release = new CountDownLatch(1);
    start(settings(4, 1, 16, true), (batch, last) -> release.await());
    for (int i = 0; i < 6; i++) {
      add(p0, i, "k" + i);
    }
    await().until(() -> c.paused().contains(p0));
    int before = c.polls.get();
    Thread.sleep(300);
    assertThat(c.polls.get()).isGreaterThan(before);
    assertThat(meters.get("chat.pipeline.paused").tag("pipeline", "test").gauge().value()).isEqualTo(1.0);
    assertThat(meters.get("chat.pipeline.oldest").tag("pipeline", "test").gauge().value()).isGreaterThan(0.0);
    release.countDown();
    await().until(() -> c.paused().isEmpty());
  }

  @Test
  void transientFailureIsRetriedInPlaceWithoutSeek() {
    List<List<Long>> calls = Collections.synchronizedList(new ArrayList<>());
    start(settings(100, 1, 16, true), (batch, last) -> {
      calls.add(offsets(batch));
      if (calls.size() <= 3) {
        throw new StoreUnavailableException("down", null);
      }
    });
    add(p0, 0, "a");
    await().pollInterval(Duration.ofMillis(5)).until(() -> calls.size() >= 1);
    c.seeks.set(0);   // MockConsumer seeks once itself, to set the first position
    await().until(() -> calls.size() == 4);
    assertThat(calls).containsOnly(List.of(0L));
    assertThat(c.seeks.get()).isZero();
    assertThat(meters.get("chat.pipeline.retries").tag("pipeline", "test").tag("reason", "transient").counter()
        .count()).isEqualTo(3.0);
    await().until(() -> c.commits.getOrDefault(p0, 0L) == 1L);
  }

  @Test
  void deterministicFailureGetsALastTryAfterMaxAttempts() {
    List<Boolean> lastTries = Collections.synchronizedList(new ArrayList<>());
    start(settings(100, 1, 16, true), (batch, last) -> {
      lastTries.add(last);
      throw new IllegalArgumentException("bad");
    });
    add(p0, 0, "a");
    await().atMost(Duration.ofSeconds(10)).until(() -> lastTries.size() >= PoisonGuard.MAX_ATTEMPTS);
    assertThat(lastTries.subList(0, PoisonGuard.MAX_ATTEMPTS - 1)).containsOnly(false);
    assertThat(lastTries.get(PoisonGuard.MAX_ATTEMPTS - 1)).isTrue();
    assertThat(meters.get("chat.pipeline.retries").tag("pipeline", "test").tag("reason", "poison").counter()
        .count()).isGreaterThanOrEqualTo(4.0);
  }

  @Test
  void noLastTryWithoutDeadLetters() {
    List<Boolean> lastTries = Collections.synchronizedList(new ArrayList<>());
    start(settings(100, 1, 16, false), (batch, last) -> {
      lastTries.add(last);
      throw new IllegalArgumentException("bad");
    });
    add(p0, 0, "a");
    await().atMost(Duration.ofSeconds(10)).until(() -> lastTries.size() >= PoisonGuard.MAX_ATTEMPTS + 1);
    assertThat(lastTries).containsOnly(false);
  }

  @Test
  void retriedBatchIsFrozen() {
    List<List<Long>> calls = Collections.synchronizedList(new ArrayList<>());
    start(settings(100, 100, 16, true), (batch, last) -> {
      calls.add(offsets(batch));
      if (calls.size() <= 2) {
        throw new StoreUnavailableException("down", null);
      }
    });
    add(p0, 0, "a");
    await().pollInterval(Duration.ofMillis(5)).until(() -> calls.size() >= 1);
    add(p0, 1, "a");
    await().until(() -> calls.stream().anyMatch(b -> b.contains(1L)));
    assertThat(calls.subList(0, 3)).containsOnly(List.of(0L));
    assertThat(calls.get(3)).containsExactly(1L);
  }

  @Test
  void nullKeysShareOneLanePerPartition() {
    List<Long> seen = Collections.synchronizedList(new ArrayList<>());
    AtomicInteger inFlight = new AtomicInteger();
    AtomicInteger most = new AtomicInteger();
    start(settings(100, 1, 16, true), (batch, last) -> {
      most.accumulateAndGet(inFlight.incrementAndGet(), Math::max);
      Thread.sleep(2);
      seen.addAll(offsets(batch));
      inFlight.decrementAndGet();
    });
    for (int i = 0; i < 20; i++) {
      add(p0, i, null);
    }
    await().until(() -> seen.size() == 20);
    assertThat(seen).isSorted();
    assertThat(most.get()).isEqualTo(1);
  }

  @Test
  void maxConcurrencyIsNeverExceeded() {
    AtomicInteger inFlight = new AtomicInteger();
    AtomicInteger most = new AtomicInteger();
    AtomicInteger done = new AtomicInteger();
    start(settings(100, 1, 3, true), (batch, last) -> {
      most.accumulateAndGet(inFlight.incrementAndGet(), Math::max);
      Thread.sleep(100);
      inFlight.decrementAndGet();
      done.incrementAndGet();
    });
    for (int i = 0; i < 10; i++) {
      add(i % 2 == 0 ? p0 : p1, i / 2, "k" + i);
    }
    await().until(() -> done.get() == 10);
    assertThat(most.get()).isEqualTo(3);
  }

  @Test
  void revokeWaitsForRunningBatchesCommitsAndCallsHook() {
    CountDownLatch started = new CountDownLatch(1);
    start(settings(100, 1, 16, true), (batch, last) -> {
      started.countDown();
      Thread.sleep(200);
    });
    add(p0, 0, "a");
    await().until(() -> started.getCount() == 0);
    c.schedulePollTask(() -> c.rebalance(List.of(p1)));
    await().until(() -> revoked.get() == 1);
    assertThat(c.commits.get(p0)).isEqualTo(1L);
  }

  @Test
  void reassignedPartitionWaitsForTheRunningBatchOfAKey() {
    CountDownLatch release = new CountDownLatch(1);
    AtomicInteger calls = new AtomicInteger();
    AtomicInteger running = new AtomicInteger();
    AtomicInteger most = new AtomicInteger();
    pipeline = new KeyedPipeline("test", c, T, new KeyedPipeline.Settings(100, 1, 16, Duration.ofMillis(50),
        Duration.ofMillis(200), true), (batch, last) -> {
          most.accumulateAndGet(running.incrementAndGet(), Math::max);
          if (calls.incrementAndGet() == 1) {
            boolean done = false;
            while (!done) {
              try {
                release.await();
                done = true;
              } catch (InterruptedException ignored) {
                // an old batch that does not stop when told
              }
            }
          }
          running.decrementAndGet();
        }, revoked::incrementAndGet, meters);
    c.schedulePollTask(() -> {
      c.rebalance(List.of(p0, p1));
      c.updateBeginningOffsets(new HashMap<>(Map.of(p0, 0L, p1, 0L)));
    });
    pipeline.start();
    await().until(() -> c.assignment().size() == 2);
    add(p0, 0, "a");
    await().until(() -> calls.get() == 1);
    c.schedulePollTask(() -> c.rebalance(List.of(p1)));
    await().until(() -> revoked.get() == 1);
    c.schedulePollTask(() -> {
      c.rebalance(List.of(p0, p1));
      add(p0, 0, "a");
      add(p0, 1, "a");
    });
    await().pollDelay(Duration.ofMillis(300)).until(() -> true);
    assertThat(calls.get()).isEqualTo(1);
    release.countDown();
    await().until(() -> calls.get() >= 2);
    assertThat(most.get()).isEqualTo(1);
  }

  @Test
  void closeStopsRetryingLanesAndCommitsWhatIsDone() {
    AtomicBoolean bDone = new AtomicBoolean();
    start(settings(100, 1, 16, true), (batch, last) -> {
      if ("a".equals(batch.getFirst().key())) {
        throw new StoreUnavailableException("down", null);
      }
      bDone.set(true);
    });
    add(p0, 0, "a");
    add(p0, 1, "b");
    await().until(bDone::get);
    long t0 = System.nanoTime();
    pipeline.close();
    pipeline = null;
    assertThat(Duration.ofNanos(System.nanoTime() - t0)).isLessThan(Duration.ofSeconds(2));
    assertThat(c.commits.getOrDefault(p0, 0L)).isZero();
  }

  @Test
  void lanesStillRunningAfterRevokeWaitAreInterrupted() {
    AtomicBoolean interrupted = new AtomicBoolean();
    CountDownLatch started = new CountDownLatch(1);
    start(settings(100, 1, 16, false), (batch, last) -> {
      started.countDown();
      try {
        while (true) {
          Thread.sleep(50);   // a handler with its own retry loop
        }
      } catch (InterruptedException e) {
        interrupted.set(true);
        throw e;
      }
    });
    add(p0, 0, "a");
    await().until(() -> started.getCount() == 0);
    long t0 = System.nanoTime();
    pipeline.close();
    pipeline = null;
    assertThat(Duration.ofNanos(System.nanoTime() - t0)).isLessThan(Duration.ofSeconds(2));
    await().until(interrupted::get);
  }
}
