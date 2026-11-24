package com.pangate.chat.grpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.pangate.chat.core.Ids;
import com.pangate.chat.core.PushDroppedException;
import com.pangate.chat.core.PushUnavailableException;
import com.pangate.chat.core.Pusher.Priority;
import com.pangate.contracts.v1.PushRequest;
import com.pangate.contracts.v1.PushResponse;
import com.pangate.contracts.v1.PushServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Perf 3b (spec §4.1, §4.3): one stream's window, priority queues and timeouts. */
class PushStreamTest {

  final ScriptedPushService api = new ScriptedPushService();
  final SimpleMeterRegistry meters = new SimpleMeterRegistry();
  final ScheduledExecutorService timers = Executors.newSingleThreadScheduledExecutor();
  Server server;
  ManagedChannel channel;
  final List<PushStream> made = new ArrayList<>();

  @BeforeEach
  void start() throws Exception {
    String name = "push-" + Ids.next();
    server = InProcessServerBuilder.forName(name).addService(api).build().start();
    channel = InProcessChannelBuilder.forName(name).build();
  }

  @AfterEach
  void stop() {
    made.forEach(PushStream::close);
    channel.shutdownNow();
    server.shutdownNow();
    timers.shutdownNow();
  }

  /** window 10, low share 0.2 (2 slots), low wait 300 ms, queue 5, must wait 10 s, timeout 100 ms. */
  PushStream stream(Duration timeout) {
    PushStream s = new PushStream(obs -> PushServiceGrpc.newStub(channel).sendStream(obs),
        new PushStream.Limits(10, 0.2, Duration.ofMillis(300), 5, Duration.ofSeconds(10), timeout, Duration.ofMinutes(5)), System::nanoTime,
        timers, meters, 0);
    made.add(s);
    return s;
  }

  PushStream stream(PushStream.Limits limits,
      java.util.function.Function<io.grpc.stub.StreamObserver<PushResponse>,
          io.grpc.stub.StreamObserver<PushRequest>> opener) {
    PushStream s = new PushStream(opener, limits, System::nanoTime, timers, meters, 0);
    made.add(s);
    return s;
  }

  static PushRequest req(String principal) {
    return PushRequest.newBuilder().setRequestId(Ids.next()).setKind(PushRequest.Kind.KIND_MESSAGE)
        .setPrincipalId(principal).build();
  }

  static Throwable cause(CompletableFuture<?> f) {
    try {
      f.get();
      return null;
    } catch (ExecutionException e) {
      return e.getCause();
    } catch (InterruptedException e) {
      throw new AssertionError(e);
    }
  }

  @Test
  void writesAtMostWindowAndQueuesTheRest() {
    PushStream s = stream(Duration.ofSeconds(5));
    for (int i = 0; i < 15; i++) {
      s.send(req("p" + i), Priority.MUST);
    }
    await().until(() -> api.received.size() == 10);
    api.answerReceived(0);
    await().until(() -> api.received.size() == 11);
    assertThat(api.outstanding()).isEqualTo(10);
  }

  @Test
  void queuedRequestDoesNotExpire() throws Exception {
    PushStream s = stream(Duration.ofMillis(500));
    for (int i = 0; i < 10; i++) {
      s.send(req("p" + i), Priority.MUST);
    }
    await().until(() -> api.received.size() == 10);
    CompletableFuture<PushResponse> queued = s.send(req("q"), Priority.MUST);
    Thread.sleep(1_000);
    assertThat(queued).isNotDone();
    api.answerReceived(0);
    await().until(() -> api.received.size() == 11);
    api.answerReceived(10);
    assertThat(queued.get().getCode()).isEqualTo(PushResponse.Code.CODE_OK);
  }

  @Test
  void timedOutRequestKeepsItsSlot() throws Exception {
    PushStream s = stream(Duration.ofMillis(100));
    List<CompletableFuture<PushResponse>> first = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      first.add(s.send(req("p" + i), Priority.MUST));
    }
    await().until(() -> first.stream().allMatch(CompletableFuture::isDone));
    assertThat(first).allSatisfy(f -> assertThat(cause(f)).isInstanceOf(PushUnavailableException.class));
    s.send(req("next"), Priority.MUST);
    Thread.sleep(120);   // under the stall limit (3 timeouts from the open)
    assertThat(api.received).hasSize(10);   // the server still counts the 10: no write past its window
    api.answerReceived(3);
    await().until(() -> api.received.size() == 11);
  }

  @Test
  void lateAnswerNeverCompletesARetry() throws Exception {
    PushStream s = stream(Duration.ofMillis(500));
    PushRequest r = req("p");
    CompletableFuture<PushResponse> first = s.send(r, Priority.MUST);
    await().until(first::isDone);
    CompletableFuture<PushResponse> second = s.send(r, Priority.MUST);
    await().until(() -> api.received.size() == 2);
    assertThat(api.received.get(0).request().getRequestId())
        .isNotEqualTo(api.received.get(1).request().getRequestId());
    api.answerReceived(0);
    Thread.sleep(50);
    assertThat(second).isNotDone();
    api.answerReceived(1);
    assertThat(second.get().getRequestId()).isEqualTo(r.getRequestId());
  }

  @Test
  void droppableIsDroppedWhenLowShareIsFull() {
    PushStream s = stream(Duration.ofSeconds(5));
    s.send(req("a"), Priority.BEST_EFFORT);
    s.send(req("b"), Priority.BEST_EFFORT);
    await().until(() -> api.received.size() == 2);
    CompletableFuture<PushResponse> d = s.send(req("c"), Priority.DROPPABLE);
    assertThat(cause(d)).isInstanceOfSatisfying(PushDroppedException.class,
        e -> assertThat(e.reason()).isEqualTo(PushDroppedException.WINDOW_FULL));
    assertThat(meters.counter("chat.push.dropped", "priority", "droppable", "reason", "window_full").count())
        .isEqualTo(1);
  }

  @Test
  void bestEffortWaitsThenIsDropped() {
    PushStream s = stream(Duration.ofSeconds(5));
    for (int i = 0; i < 10; i++) {
      s.send(req("p" + i), Priority.MUST);
    }
    await().until(() -> api.received.size() == 10);
    long t0 = System.nanoTime();
    CompletableFuture<PushResponse> b = s.send(req("b"), Priority.BEST_EFFORT);
    assertThat(cause(b)).isInstanceOfSatisfying(PushDroppedException.class,
        e -> assertThat(e.reason()).isEqualTo(PushDroppedException.WAITED));
    assertThat(Duration.ofNanos(System.nanoTime() - t0)).isBetween(Duration.ofMillis(250), Duration.ofMillis(2_000));
  }

  @Test
  void mustGoesBeforeBestEffort() {
    PushStream s = stream(Duration.ofSeconds(5));
    for (int i = 0; i < 10; i++) {
      s.send(req("p" + i), Priority.MUST);
    }
    await().until(() -> api.received.size() == 10);
    s.send(req("best"), Priority.BEST_EFFORT);
    s.send(req("must"), Priority.MUST);
    api.answerReceived(0);
    await().until(() -> api.received.size() == 11);
    assertThat(api.received.get(10).request().getPrincipalId()).isEqualTo("must");
  }

  @Test
  void lowPrioritiesNeverExceedTheirShare() {
    PushStream s = stream(Duration.ofSeconds(5));
    for (int i = 0; i < 1_000; i++) {
      s.send(req("t" + i), i % 2 == 0 ? Priority.DROPPABLE : Priority.BEST_EFFORT);
    }
    await().until(() -> api.received.size() >= 2);
    assertThat(api.outstanding()).isLessThanOrEqualTo(2);
    for (int i = 0; i < 8; i++) {
      s.send(req("m" + i), Priority.MUST);
    }
    await().until(() -> api.requests().stream().filter(r -> r.getPrincipalId().startsWith("m")).count() == 8);
  }

  @Test
  void queueFullRefusesAtOnce() {
    PushStream s = stream(Duration.ofSeconds(5));
    for (int i = 0; i < 15; i++) {
      s.send(req("p" + i), Priority.MUST);
    }
    CompletableFuture<PushResponse> over = s.send(req("over"), Priority.MUST);
    assertThat(cause(over)).isInstanceOf(PushUnavailableException.class);
    for (int i = 0; i < 5; i++) {
      s.send(req("b" + i), Priority.BEST_EFFORT);
    }
    CompletableFuture<PushResponse> b = s.send(req("b-over"), Priority.BEST_EFFORT);
    assertThat(cause(b)).isInstanceOfSatisfying(PushDroppedException.class,
        e -> assertThat(e.reason()).isEqualTo(PushDroppedException.QUEUE_FULL));
  }

  /** The transport is not ready between messages: the stream still drains its queue (onReady). */
  @Test
  void drainsWhenTheTransportComesBackReady() {
    api.oneAtATime = true;
    PushStream s = stream(Duration.ofSeconds(5));
    for (int i = 0; i < 15; i++) {
      s.send(req("p" + i), Priority.MUST);
    }
    await().until(() -> api.received.size() == 10);
    for (int i = 0; i < 5; i++) {
      api.answerReceived(i);
    }
    await().until(() -> api.received.size() == 15);
  }

  // --- Task 3: the stream's life (spec §4.2) ---

  @Test
  void endedStreamFailsInFlightAndResendsQueued() {
    PushStream s = stream(Duration.ofSeconds(5));
    List<CompletableFuture<PushResponse>> written = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      written.add(s.send(req("p" + i), Priority.MUST));
    }
    for (int i = 0; i < 5; i++) {
      s.send(req("q" + i), Priority.MUST);
    }
    await().until(() -> api.received.size() == 10);
    api.endAll();
    await().until(() -> written.stream().allMatch(CompletableFuture::isDone));
    assertThat(written).allSatisfy(f -> assertThat(cause(f)).isInstanceOf(PushUnavailableException.class));
    await().until(() -> api.received.stream().filter(r -> r.stream() == 2).count() == 5);
    assertThat(meters.counter("chat.push.stream.restarts").count()).isPositive();
  }

  @Test
  void mustFailsAfterMustWaitWhenNoStreamOpens() {
    PushStream s = stream(new PushStream.Limits(10, 0.2, Duration.ofMillis(300), 5, Duration.ofMillis(300),
        Duration.ofSeconds(5), Duration.ofMinutes(5)), obs -> {
          throw new IllegalStateException("no push-api");
        });
    long t0 = System.nanoTime();
    CompletableFuture<PushResponse> f = s.send(req("p"), Priority.MUST);
    assertThat(cause(f)).isInstanceOf(PushUnavailableException.class);
    assertThat(Duration.ofNanos(System.nanoTime() - t0)).isBetween(Duration.ofMillis(250), Duration.ofMillis(1_500));
    assertThat(meters.counter("chat.push.dropped", "priority", "must", "reason", "must_wait").count()).isEqualTo(1);
  }

  @Test
  void reopenBacksOff() throws Exception {
    java.util.concurrent.atomic.AtomicInteger opens = new java.util.concurrent.atomic.AtomicInteger();
    PushStream s = stream(new PushStream.Limits(10, 0.2, Duration.ofMillis(300), 5, Duration.ofSeconds(10),
        Duration.ofSeconds(5), Duration.ofMinutes(5)), obs -> {
          opens.incrementAndGet();
          throw new IllegalStateException("no push-api");
        });
    s.send(req("p"), Priority.MUST);
    for (int i = 0; i < 200; i++) {
      s.send(req("x" + i), Priority.DROPPABLE);   // every send would try to open
      Thread.sleep(10);
    }
    assertThat(opens.get()).isBetween(2, 6);
  }

  @Test
  void idleStreamIsNotReopenedUntilNeeded() throws Exception {
    PushStream s = stream(Duration.ofSeconds(5));
    CompletableFuture<PushResponse> f = s.send(req("p"), Priority.MUST);
    await().until(() -> api.received.size() == 1);
    api.answerReceived(0);
    f.get();
    api.endAll();
    Thread.sleep(300);
    assertThat(api.streams.get()).isEqualTo(1);
    s.send(req("q"), Priority.MUST);
    await().until(() -> api.streams.get() == 2);
  }

  // --- final review fixes ---

  PushStream stream(Duration timeout, Duration mustWait, Duration maxAge) {
    return stream(new PushStream.Limits(10, 0.2, Duration.ofMillis(300), 50, mustWait, timeout, maxAge),
        obs -> PushServiceGrpc.newStub(channel).sendStream(obs));
  }

  /** I1: a request queued while the stream backs off is written when the backoff ends, with no other traffic. */
  @Test
  void queuedDuringBackoffIsWrittenWhenBackoffEnds() {
    PushStream s = stream(Duration.ofSeconds(5));
    s.send(req("a"), Priority.MUST);
    await().until(() -> api.received.size() == 1);
    api.endAll();   // ended before any answer: the stream backs off
    await().until(() -> meters.counter("chat.push.stream.restarts").count() > 0);
    api.autoAnswer = true;
    CompletableFuture<PushResponse> b = s.send(req("b"), Priority.MUST);
    await().atMost(Duration.ofSeconds(3)).until(() -> api.requests().stream()
        .anyMatch(r -> r.getPrincipalId().equals("b")));
    await().until(b::isDone);
    assertThat(cause(b)).isNull();
  }

  /** I2: the client rotates its call before the server's connection age: nothing in flight fails. */
  @Test
  void rotationIsGraceful() throws Exception {
    api.autoAnswer = true;
    PushStream s = stream(Duration.ofSeconds(5), Duration.ofSeconds(10), Duration.ofMillis(300));
    List<CompletableFuture<PushResponse>> all = new ArrayList<>();
    for (int i = 0; i < 150; i++) {
      all.add(s.send(req("p" + i), Priority.MUST));
      Thread.sleep(10);
    }
    await().until(() -> all.stream().allMatch(CompletableFuture::isDone));
    assertThat(all).allSatisfy(f -> assertThat(cause(f)).isNull());
    assertThat(api.streams.get()).isGreaterThanOrEqualTo(3);
  }

  /** I3: a call that answers nothing for 3 timeouts while every request on it timed out is cancelled, replaced. */
  @Test
  void stalledCallIsCancelledAndReplaced() {
    PushStream s = stream(Duration.ofMillis(100));
    for (int i = 0; i < 12; i++) {
      s.send(req("p" + i), Priority.MUST);
    }
    await().until(() -> api.received.size() == 10);
    await().atMost(Duration.ofSeconds(3)).until(() -> api.received.stream().anyMatch(r -> r.stream() == 2));
  }

  /**
   * Codex review of PR #11: a call rotated out while its requests wait on a wedged server is cancelled like a current
   * one, so its slots come back and queued requests reach a new call.
   */
  @Test
  void rotatedCallThatStallsIsCancelled() {
    api.wedged = true;
    PushStream s = stream(Duration.ofMillis(100), Duration.ofSeconds(10), Duration.ofMillis(200));
    for (int i = 0; i < 10; i++) {
      s.send(req("p" + i), Priority.MUST);
    }
    await().until(() -> api.received.size() == 10);
    await().until(() -> meters.counter("chat.push.stream.rotations").count() >= 1);
    for (int i = 10; i < 15; i++) {
      s.send(req("p" + i), Priority.MUST);
    }
    await().atMost(Duration.ofSeconds(3)).until(() -> api.received.stream().anyMatch(r -> r.stream() == 2));
    assertThat(meters.counter("chat.push.stream.stalled").count()).isGreaterThanOrEqualTo(1);
  }

  /** I4: must-wait does not fail requests while the stream keeps answering, however slowly. */
  @Test
  void mustDoesNotExpireWhileTheStreamProgresses() throws Exception {
    PushStream s = stream(Duration.ofSeconds(5), Duration.ofMillis(300), Duration.ofMinutes(5));
    List<CompletableFuture<PushResponse>> all = new ArrayList<>();
    for (int i = 0; i < 30; i++) {
      all.add(s.send(req("p" + i), Priority.MUST));
    }
    Thread answerer = Thread.ofPlatform().daemon().start(() -> {
      while (!Thread.currentThread().isInterrupted()) {
        api.answerOldest();
        try {
          Thread.sleep(50);
        } catch (InterruptedException e) {
          return;
        }
      }
    });
    try {
      await().atMost(Duration.ofSeconds(10)).until(() -> all.stream().allMatch(CompletableFuture::isDone));
      assertThat(all).allSatisfy(f -> assertThat(cause(f)).isNull());
    } finally {
      answerer.interrupt();
    }
  }

  /** I5: with no Push API at all, queued musts wait for must-wait instead of failing on the first dead call. */
  @Test
  void deadAddressFailsByMustWaitNotAtOnce() throws Exception {
    int port;
    try (java.net.ServerSocket ss = new java.net.ServerSocket(0)) {
      port = ss.getLocalPort();
    }
    ManagedChannel dead = io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder.forAddress("127.0.0.1", port)
        .usePlaintext().build();
    try {
      PushStream s = stream(new PushStream.Limits(10, 0.2, Duration.ofMillis(300), 5, Duration.ofSeconds(1),
          Duration.ofSeconds(5), Duration.ofMinutes(5)), obs -> PushServiceGrpc.newStub(dead).sendStream(obs));
      List<CompletableFuture<PushResponse>> all = new ArrayList<>();
      for (int i = 0; i < 3; i++) {
        all.add(s.send(req("p" + i), Priority.MUST));
      }
      Thread.sleep(600);
      assertThat(all).noneMatch(CompletableFuture::isDone);
      await().atMost(Duration.ofSeconds(3)).until(() -> all.stream().allMatch(CompletableFuture::isDone));
      assertThat(all).allSatisfy(f -> assertThat(cause(f)).isInstanceOf(PushUnavailableException.class));
    } finally {
      dead.shutdownNow();
    }
  }
}
