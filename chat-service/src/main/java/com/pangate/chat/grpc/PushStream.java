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
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.LongSupplier;

/**
 * Perf 3b (spec §4.1–§4.3): one long-lived {@code PushService.SendStream} with a client-side window. At most
 * {@code window} requests wait for an answer (the server's own window, so nothing piles up in gRPC buffers); the rest
 * wait in a queue per priority. MUST may use the whole window and leaves the queue first; BEST_EFFORT and DROPPABLE
 * together use at most {@code lowShare} of it. A DROPPABLE that finds no room is dropped at once, a BEST_EFFORT that
 * waited {@code lowWait} is dropped. What sits in gRPC's own buffers is bounded by the window. The timeout starts when a request is written; one that timed out keeps its slot
 * until the server answers it (the server still counts it). Every write carries a fresh request id on the wire, so a
 * late answer to an earlier try never completes a retry.
 */
final class PushStream implements AutoCloseable {

  static final long MIN_BACKOFF_MS = 100;
  static final long MAX_BACKOFF_MS = 5_000;

  /** The limits of one stream (spec §4.5). */
  record Limits(int window, double lowShare, Duration lowWait, int queueMax, Duration mustWait, Duration timeout) {
    int lowSlots() {
      return Math.max(1, (int) Math.floor(window * lowShare));
    }
  }

  private static final class Item {
    final PushRequest request;
    final Priority priority;
    final CompletableFuture<PushResponse> reply;
    final long enqueuedNanos;
    String wireId;
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

  /** One open call; replaced when it ends. */
  private final class Call implements ClientResponseObserver<PushRequest, PushResponse> {
    ClientCallStreamObserver<PushRequest> requests;
    /** Whether the server ever answered on this call: one that ends without an answer counts as a failed open. */
    volatile boolean answeredOnce;

    @Override
    public void beforeStart(ClientCallStreamObserver<PushRequest> requestStream) {
      this.requests = requestStream;
    }

    @Override
    public void onNext(PushResponse v) {
      answeredOnce = true;
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
        if (lowQueue.size() >= limits.queueMax()) {
          drop(item, PushDroppedException.QUEUE_FULL, after);
        } else {
          lowQueue.add(item);
        }
      }
      case MUST -> {
        if (mustQueue.size() >= limits.queueMax()) {
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
    if (item.priority == Priority.BEST_EFFORT) {
      timers.schedule(() -> expireLow(item), limits.lowWait().toNanos(), TimeUnit.NANOSECONDS);
    } else if (item.priority == Priority.MUST) {
      timers.schedule(() -> expireMust(item), limits.mustWait().toNanos(), TimeUnit.NANOSECONDS);
    }
  }

  private void expireMust(Item item) {
    synchronized (this) {
      if (!mustQueue.remove(item)) {
        return;
      }
      count("must", "must_wait");
    }
    item.reply.completeExceptionally(new PushUnavailableException(
        "no push stream for " + limits.mustWait() + " (push-api down?)", null));
  }

  private boolean canWrite(boolean low) {
    if (inFlight.size() >= limits.window() || (low && lowInFlight >= limits.lowSlots())) {
      return false;
    }
    // gRPC's isReady() is not consulted: the window already bounds what can sit in gRPC's buffers to `window`
    // requests, and the Push API requests its whole window up front (PushGrpcService), so it never holds them back
    return open() != null;
  }

  /** The open call, opening one when there is none. Null when it cannot be opened now (backing off). */
  private Call open() {
    if (call == null && !closed && nanos.getAsLong() >= nextOpenNanos) {
      Call c = new Call();
      try {
        opener.apply(c);
        call = c;
      } catch (RuntimeException e) {
        backOff();
      }
    }
    return call;
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
  }

  private void write(Item item, List<Runnable> after) {
    item.wireId = Ids.next();
    inFlight.put(item.wireId, item);
    if (item.low()) {
      lowInFlight++;
    }
    meters.timer("chat.push.queue.wait", "priority", name(item.priority))
        .record(nanos.getAsLong() - item.enqueuedNanos, TimeUnit.NANOSECONDS);
    long writtenAt = nanos.getAsLong();
    item.writtenNanos = writtenAt;
    Call c = call;
    try {
      c.requests.onNext(item.request.toBuilder().setRequestId(item.wireId).build());
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
      if (lowQueue.remove(item)) {
        drop(item, PushDroppedException.WAITED, after);
      }
    }
    after.forEach(Runnable::run);
  }

  /** No answer in time: the caller hears now, but the slot stays taken until the server answers. */
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
  }

  private void answered(Call c, PushResponse v) {
    List<Runnable> after = new ArrayList<>();
    synchronized (this) {
      Item item = inFlight.get(v.getRequestId());
      if (item != null && call == c) {
        release(item);
        latency.record(nanos.getAsLong() - item.writtenNanos, TimeUnit.NANOSECONDS);
        PushResponse answer = v.toBuilder().setRequestId(item.request.getRequestId()).build();
        after.add(() -> item.reply.complete(answer));
      }
      drain(after);
    }
    after.forEach(Runnable::run);
  }

  /** The call ended: what waits for an answer on it fails now; the queues stay for the next call. */
  private void ended(Call c, Throwable t) {
    List<Runnable> after = new ArrayList<>();
    synchronized (this) {
      if (call != c) {
        return;
      }
      call = null;
      for (Item item : inFlight.values()) {
        after.add(() -> item.reply.completeExceptionally(new PushUnavailableException(t.toString(), t)));
      }
      inFlight.clear();
      lowInFlight = 0;
      meters.counter("chat.push.stream.restarts").increment();
      if (c.answeredOnce) {
        backoffMs = 0;
        nextOpenNanos = 0;
        reopenSoon();
      } else {
        backOff();
      }
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
