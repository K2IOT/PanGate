package com.pangate.chat.grpc;

import com.pangate.chat.core.Ids;
import com.pangate.chat.core.PushDroppedException;
import com.pangate.chat.core.PushUnavailableException;
import com.pangate.chat.core.Pusher.Priority;
import com.pangate.contracts.v1.PushRequest;
import com.pangate.contracts.v1.PushResponse;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.LongSupplier;

/**
 * Perf 3b (spec §4.1–§4.3): one long-lived {@code PushService.SendStream} with a client-side window. At most
 * {@code window} requests wait for an answer (the server's own window, so the count of what can sit in gRPC's buffers
 * is bounded); the rest wait in a queue per priority. MUST may use the whole window and leaves the queue first;
 * BEST_EFFORT and DROPPABLE together use at most {@code lowShare} of it. A DROPPABLE that finds no room is dropped at
 * once, a BEST_EFFORT that waited {@code lowWait} is dropped, a MUST fails after {@code mustWait} without progress.
 * The timeout starts when a request is written; one that timed out keeps its slot until the server answers it (the
 * server still counts it). Every write carries a fresh request id on the wire, so a late answer to an earlier try
 * never completes a retry.
 *
 * <p>Final review of perf 3b: a call is used only once gRPC reports it ready (so a dead Push API fails no queued
 * request, they wait for must-wait); the client rotates its call every {@code maxAge} (jittered), half-closing the old
 * one so the server answers what it holds before the server's connection age cuts it; a call on which every request
 * timed out and nothing was answered for {@link #STALL_TIMEOUTS} timeouts is cancelled, freeing its slots.
 */
final class PushStream implements AutoCloseable {

  static final long MIN_BACKOFF_MS = 100;
  static final long MAX_BACKOFF_MS = 5_000;
  /** A call quiet for this many timeouts, with every request on it timed out, is cancelled (final review I3). */
  static final int STALL_TIMEOUTS = 3;

  /** The limits of one stream (spec §4.5). */
  record Limits(int window, double lowShare, Duration lowWait, int queueMax, Duration mustWait, Duration timeout,
      Duration maxAge) {
    int lowSlots() {
      return Math.max(1, (int) Math.floor(window * lowShare));
    }
  }

  private static final class Item {
    final PushRequest request;
    final Priority priority;
    final CompletableFuture<PushResponse> reply;
    final long enqueuedNanos;
    long waitArmedNanos;
    String wireId;
    Call call;
    boolean timedOut;
    long writtenNanos;

    Item(PushRequest request, Priority priority, CompletableFuture<PushResponse> reply, long enqueuedNanos) {
      this.request = request;
      this.priority = priority;
      this.reply = reply;
      this.enqueuedNanos = enqueuedNanos;
    }

    boolean low() {
      return priority != Priority.MUST;
    }
  }

  /** One call; replaced when it ends or when it is rotated. */
  private final class Call implements ClientResponseObserver<PushRequest, PushResponse> {
    ClientCallStreamObserver<PushRequest> requests;
    final long openedNanos = nanos.getAsLong();
    /** gRPC said the call is ready: before that nothing is written, so a call that never starts fails nothing. */
    volatile boolean started;
    volatile boolean answeredOnce;
    volatile long lastAnswerNanos;

    @Override
    public void beforeStart(ClientCallStreamObserver<PushRequest> requestStream) {
      this.requests = requestStream;
      requestStream.setOnReadyHandler(() -> {
        if (!started) {
          started = true;
          drainAndComplete();
        }
      });
    }

    @Override
    public void onNext(PushResponse v) {
      answeredOnce = true;
      lastAnswerNanos = nanos.getAsLong();
      answered(this, v);
    }

    @Override
    public void onError(Throwable t) {
      ended(this, t);
    }

    @Override
    public void onCompleted() {
      ended(this, new IllegalStateException("push-api closed the stream"));
    }
  }

  private final Function<StreamObserver<PushResponse>, StreamObserver<PushRequest>> opener;
  private final Limits limits;
  private final LongSupplier nanos;
  private final ScheduledExecutorService timers;
  private final MeterRegistry meters;
  private final String index;
  private final Timer latency;

  // guarded by this
  private Call call;
  private final Map<String, Item> inFlight = new HashMap<>();
  private int lowInFlight;
  private final ArrayDeque<Item> mustQueue = new ArrayDeque<>();
  private final ArrayDeque<Item> lowQueue = new ArrayDeque<>();
  private boolean closed;
  /** Reopen backoff (spec §4.2): 0 while healthy, else 100 ms doubling to 5 s. */
  private long backoffMs;
  private long nextOpenNanos;
  private boolean reopenScheduled;
  /** The last answer on any call of this stream: must-wait only fails a request when nothing moved. */
  private long lastProgressNanos;

  /** {@code opener} starts a new {@code SendStream} call with the given response observer. */
  PushStream(Function<StreamObserver<PushResponse>, StreamObserver<PushRequest>> opener, Limits limits,
      LongSupplier nanos, ScheduledExecutorService timers, MeterRegistry meters, int index) {
    this.opener = opener;
    this.limits = limits;
    this.nanos = nanos;
    this.timers = timers;
    this.meters = meters;
    this.index = Integer.toString(index);
    this.latency = meters.timer("chat.push.latency");
    Gauge.builder("chat.push.inflight", this, PushStream::inFlightCount).tag("stream", this.index).register(meters);
    Gauge.builder("chat.push.queued", this, s -> s.queued(true)).tags("priority", "must", "stream", this.index)
        .register(meters);
    Gauge.builder("chat.push.queued", this, s -> s.queued(false)).tags("priority", "best_effort", "stream", this.index)
        .register(meters);
  }

  private synchronized double inFlightCount() {
    return inFlight.size();
  }

  private synchronized double queued(boolean must) {
    return must ? mustQueue.size() : lowQueue.size();
  }

  CompletableFuture<PushResponse> send(PushRequest r, Priority p) {
    CompletableFuture<PushResponse> reply = new CompletableFuture<>();
    Item item = new Item(r, p, reply, nanos.getAsLong());
    List<Runnable> after = new ArrayList<>();
    synchronized (this) {
      if (closed) {
        after.add(() -> reply.completeExceptionally(new PushUnavailableException("push client closed", null)));
      } else {
        admit(item, after);
        drain(after);
        armWaits(item);
      }
    }
    after.forEach(Runnable::run);
    return reply;
  }

  /** Writes at once, queues, or refuses (under the lock; completions go to {@code after}). */
  private void admit(Item item, List<Runnable> after) {
    switch (item.priority) {
      case DROPPABLE -> {
        if (mustQueue.isEmpty() && lowQueue.isEmpty() && canWrite(true)) {
          write(item, after);
        } else {
          drop(item, PushDroppedException.WINDOW_FULL, after);
        }
      }
      case BEST_EFFORT -> {
        // what the window could take now does not count against the queue (a call not started yet holds all)
        int room = Math.max(0, Math.min(limits.lowSlots() - lowInFlight, limits.window() - inFlight.size()));
        if (lowQueue.size() >= limits.queueMax() + room) {
          drop(item, PushDroppedException.QUEUE_FULL, after);
        } else {
          lowQueue.add(item);
        }
      }
      case MUST -> {
        if (mustQueue.size() >= limits.queueMax() + Math.max(0, limits.window() - inFlight.size())) {
          count("must", "queue_full");
          after.add(() -> item.reply.completeExceptionally(
              new PushUnavailableException("push queue full (" + limits.queueMax() + ")", null)));
        } else {
          mustQueue.add(item);
        }
      }
    }
  }

  /** A request still queued after the first drain gets its wait limit: low-wait or must-wait. */
  private void armWaits(Item item) {
    if (item.wireId != null || item.reply.isDone()) {
      return;
    }
    item.waitArmedNanos = nanos.getAsLong();
    if (item.priority == Priority.BEST_EFFORT) {
      timers.schedule(() -> expireLow(item), limits.lowWait().toNanos(), TimeUnit.NANOSECONDS);
    } else if (item.priority == Priority.MUST) {
      timers.schedule(() -> expireMust(item), limits.mustWait().toNanos(), TimeUnit.NANOSECONDS);
    }
  }

  /**
   * Must-wait ran out. Final review I4: when the stream answered something meanwhile it is alive, only busy, so the
   * request keeps its place and waits again; it fails only when nothing moved (Push API down, no call starts).
   */
  private void expireMust(Item item) {
    synchronized (this) {
      if (item.wireId != null || item.reply.isDone() || !mustQueue.contains(item)) {
        return;
      }
      if (lastProgressNanos > item.waitArmedNanos) {
        item.waitArmedNanos = nanos.getAsLong();
        timers.schedule(() -> expireMust(item), limits.mustWait().toNanos(), TimeUnit.NANOSECONDS);
        return;
      }
      mustQueue.remove(item);
      count("must", "must_wait");
    }
    item.reply.completeExceptionally(new PushUnavailableException(
        "no push stream for " + limits.mustWait() + " (push-api down?)", null));
  }

  private boolean canWrite(boolean low) {
    if (inFlight.size() >= limits.window() || (low && lowInFlight >= limits.lowSlots())) {
      return false;
    }
    // gRPC's isReady() is not consulted after the start: the window already bounds what can sit in gRPC's buffers
    // to `window` requests, and the Push API requests its whole window up front (PushGrpcService)
    Call c = open();
    return c != null && c.started;
  }

  /** The open call, opening one when there is none. Null when it cannot be opened now (backing off). */
  private Call open() {
    if (call == null && !closed && nanos.getAsLong() >= nextOpenNanos) {
      Call c = new Call();
      try {
        opener.apply(c);
        call = c;
        long age = limits.maxAge().toNanos();
        long jittered = age - age / 5 + ThreadLocalRandom.current().nextLong(Math.max(1, age * 2 / 5));
        timers.schedule(() -> rotate(c), jittered, TimeUnit.NANOSECONDS);
      } catch (RuntimeException e) {
        backOff();
      }
    }
    return call;
  }

  /**
   * Final review I2: a call reached its age. New writes go to a new call; the old one is half-closed, so the server
   * answers what it holds and completes it, well before the server's own connection age cuts the connection.
   */
  private void rotate(Call c) {
    List<Runnable> after = new ArrayList<>();
    synchronized (this) {
      if (call != c) {
        return;
      }
      call = null;
      meters.counter("chat.push.stream.rotations").increment();
      after.add(() -> {
        try {
          c.requests.onCompleted();
        } catch (RuntimeException e) {
          // already gone
        }
      });
      drain(after);
    }
    after.forEach(Runnable::run);
  }

  /** A failed open or a call that ended before any answer: wait longer before the next open. */
  private void backOff() {
    backoffMs = backoffMs == 0 ? MIN_BACKOFF_MS : Math.min(MAX_BACKOFF_MS, backoffMs * 2);
    nextOpenNanos = nanos.getAsLong() + TimeUnit.MILLISECONDS.toNanos(backoffMs);
    reopenSoon();
  }

  /** Queued requests get a new call as soon as one may be opened. */
  private void reopenSoon() {
    if (reopenScheduled || closed || (mustQueue.isEmpty() && lowQueue.isEmpty())) {
      return;
    }
    reopenScheduled = true;
    long delay = Math.max(0, nextOpenNanos - nanos.getAsLong());
    timers.schedule(() -> {
      synchronized (this) {
        reopenScheduled = false;
      }
      drainAndComplete();
    }, delay, TimeUnit.NANOSECONDS);
  }

  /** Moves queued requests onto the stream while there is room: MUST first, then BEST_EFFORT within its share. */
  private void drain(List<Runnable> after) {
    while (!mustQueue.isEmpty() && canWrite(false)) {
      Item i = mustQueue.poll();
      if (!i.reply.isDone()) {
        write(i, after);
      }
    }
    while (mustQueue.isEmpty() && !lowQueue.isEmpty() && canWrite(true)) {
      Item i = lowQueue.poll();
      if (!i.reply.isDone()) {
        write(i, after);
      }
    }
    // final review I1: requests left waiting while no call is usable get a drain when one may open again
    if (call == null && !(mustQueue.isEmpty() && lowQueue.isEmpty())) {
      reopenSoon();
    }
  }

  private void write(Item item, List<Runnable> after) {
    item.wireId = Ids.next();
    item.call = call;
    inFlight.put(item.wireId, item);
    if (item.low()) {
      lowInFlight++;
    }
    meters.timer("chat.push.queue.wait", "priority", name(item.priority))
        .record(nanos.getAsLong() - item.enqueuedNanos, TimeUnit.NANOSECONDS);
    item.writtenNanos = nanos.getAsLong();
    try {
      item.call.requests.onNext(item.request.toBuilder().setRequestId(item.wireId).build());
    } catch (RuntimeException e) {
      release(item);
      after.add(() -> item.reply.completeExceptionally(new PushUnavailableException(e.toString(), e)));
      return;
    }
    String wire = item.wireId;
    timers.schedule(() -> timedOut(wire), limits.timeout().toNanos(), TimeUnit.NANOSECONDS);
  }

  private void release(Item item) {
    inFlight.remove(item.wireId);
    if (item.low()) {
      lowInFlight--;
    }
  }

  private void drop(Item item, String reason, List<Runnable> after) {
    count(name(item.priority), reason);
    after.add(() -> item.reply.completeExceptionally(new PushDroppedException(reason)));
  }

  private void count(String priority, String reason) {
    meters.counter("chat.push.dropped", "priority", priority, "reason", reason).increment();
  }

  private static String name(Priority p) {
    return p.name().toLowerCase(Locale.ROOT);
  }

  private void expireLow(Item item) {
    List<Runnable> after = new ArrayList<>();
    synchronized (this) {
      if (item.wireId == null && !item.reply.isDone() && lowQueue.remove(item)) {
        drop(item, PushDroppedException.WAITED, after);
      }
    }
    after.forEach(Runnable::run);
  }

  /**
   * No answer in time: the caller hears now, but the slot stays taken until the server answers. Final review I3: when
   * every request on a call (current or rotated out) has timed out and the call answered nothing for {@link #STALL_TIMEOUTS}
   * timeouts, it is cancelled,
   * which frees its slots ({@link #ended}) and lets round-robin place the next call elsewhere.
   */
  private void timedOut(String wireId) {
    Item item;
    synchronized (this) {
      item = inFlight.get(wireId);
      if (item == null || item.timedOut) {
        return;
      }
      item.timedOut = true;
    }
    item.reply.completeExceptionally(new PushUnavailableException(
        "push-api did not answer in " + limits.timeout(), null));
    checkStalled(item.call);
  }

  /**
   * Cancels {@code c} when it is stalled; re-checks later when it is quiet but not for long enough yet. A call rotated
   * out is checked too (Codex review of PR #11): its unanswered requests hold slots of this stream.
   */
  private void checkStalled(Call c) {
    boolean stalled;
    synchronized (this) {
      if (inFlight.values().stream().noneMatch(i -> i.call == c)
          || !inFlight.values().stream().filter(i -> i.call == c).allMatch(i -> i.timedOut)) {
        return;
      }
      long quietFor = nanos.getAsLong() - Math.max(c.openedNanos, c.lastAnswerNanos);
      long limit = STALL_TIMEOUTS * limits.timeout().toNanos();
      stalled = quietFor >= limit;
      if (!stalled) {
        timers.schedule(() -> checkStalled(c), limit - quietFor, TimeUnit.NANOSECONDS);
      }
    }
    if (stalled) {
      meters.counter("chat.push.stream.stalled").increment();
      try {
        c.requests.cancel("no answer for " + limits.timeout().multipliedBy(STALL_TIMEOUTS), null);
      } catch (RuntimeException e) {
        // already gone
      }
    }
  }

  /** An answer, on the current call or on one being rotated out: both hold slots of this stream. */
  private void answered(Call c, PushResponse v) {
    List<Runnable> after = new ArrayList<>();
    synchronized (this) {
      lastProgressNanos = nanos.getAsLong();
      Item item = inFlight.get(v.getRequestId());
      if (item != null && item.call == c) {
        release(item);
        latency.record(nanos.getAsLong() - item.writtenNanos, TimeUnit.NANOSECONDS);
        if (!item.timedOut) {
          PushResponse answer = v.toBuilder().setRequestId(item.request.getRequestId()).build();
          after.add(() -> item.reply.complete(answer));
        }
      }
      drain(after);
    }
    after.forEach(Runnable::run);
  }

  /** A call ended: what waits for an answer on it fails now; the queues stay for the next call. */
  private void ended(Call c, Throwable t) {
    List<Runnable> after = new ArrayList<>();
    synchronized (this) {
      List<Item> lost = new ArrayList<>();
      for (Item item : inFlight.values()) {
        if (item.call == c) {
          lost.add(item);
        }
      }
      for (Item item : lost) {
        release(item);
        after.add(() -> item.reply.completeExceptionally(new PushUnavailableException(t.toString(), t)));
      }
      if (call == c) {
        call = null;
        meters.counter("chat.push.stream.restarts").increment();
        if (c.answeredOnce) {
          backoffMs = 0;
          nextOpenNanos = 0;
        } else {
          backOff();
        }
      }
      drain(after);
    }
    after.forEach(Runnable::run);
  }

  private void drainAndComplete() {
    List<Runnable> after = new ArrayList<>();
    synchronized (this) {
      drain(after);
    }
    after.forEach(Runnable::run);
  }

  @Override
  public void close() {
    List<Runnable> after = new ArrayList<>();
    synchronized (this) {
      closed = true;
      Call c = call;
      call = null;
      List<Item> all = new ArrayList<>(inFlight.values());
      all.addAll(mustQueue);
      all.addAll(lowQueue);
      inFlight.clear();
      mustQueue.clear();
      lowQueue.clear();
      for (Item i : all) {
        after.add(() -> i.reply.completeExceptionally(new PushUnavailableException("push client closed", null)));
      }
      if (c != null && c.requests != null) {
        after.add(() -> {
          try {
            c.requests.onCompleted();
          } catch (RuntimeException e) {
            // already gone
          }
        });
      }
    }
    after.forEach(Runnable::run);
  }
}
