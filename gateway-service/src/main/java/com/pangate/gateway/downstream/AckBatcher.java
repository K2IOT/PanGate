package com.pangate.gateway.downstream;

import com.pangate.contracts.v1.AckItem;
import com.pangate.contracts.v1.DeliveryAck;
import com.pangate.gateway.metrics.GatewayMetrics;
import io.grpc.stub.ServerCallStreamObserver;
import io.micrometer.core.instrument.Counter;
import java.util.EnumMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Acks of one router stream (spec §6.3): any thread offers; only {@link #flush()} on the gRPC executor calls
 * {@code onNext}, every 2 ms or as soon as 256 are queued, and only while the stream is ready.
 */
public final class AckBatcher {

  static final int BATCH = 256;
  private static final long PERIOD_MICROS = 2_000;

  private final ServerCallStreamObserver<DeliveryAck> out;
  private final ScheduledExecutorService exec;
  private final ConcurrentLinkedQueue<AckItem> queue = new ConcurrentLinkedQueue<>();
  private final AtomicInteger size = new AtomicInteger();
  private final AtomicBoolean flushing = new AtomicBoolean();
  private final Map<AckItem.Status, Counter> counters = new EnumMap<>(AckItem.Status.class);
  private volatile ScheduledFuture<?> periodic;
  private volatile boolean stopped;

  public AckBatcher(ServerCallStreamObserver<DeliveryAck> out, ScheduledExecutorService exec, GatewayMetrics m) {
    this.out = out;
    this.exec = exec;
    for (AckItem.Status s : AckItem.Status.values()) {
      if (s != AckItem.Status.UNRECOGNIZED) {
        counters.put(s, m.counter("gw.delivery.acks", "status",
            s.name().substring("STATUS_".length()).toLowerCase(Locale.ROOT)));
      }
    }
  }

  public void offer(AckItem item) {
    if (stopped) {
      return;
    }
    Counter c = counters.get(item.getStatus());
    if (c != null) {
      c.increment();
    }
    queue.add(item);
    if (size.incrementAndGet() >= BATCH) {
      exec.execute(this::flush);
    }
  }

  public void start() {
    out.setOnReadyHandler(() -> exec.execute(this::flush));
    periodic = exec.scheduleAtFixedRate(this::flush, PERIOD_MICROS, PERIOD_MICROS, TimeUnit.MICROSECONDS);
  }

  public void stop() {
    stopped = true;
    ScheduledFuture<?> p = periodic;
    if (p != null) {
      p.cancel(false);
    }
    queue.clear();
    size.set(0);
  }

  /** Stops batching; with {@code complete}, ends the response stream after any flush in progress. */
  public void close(boolean complete) {
    stop();
    exec.execute(() -> {
      while (!flushing.compareAndSet(false, true)) {
        Thread.onSpinWait();
      }
      try {
        if (complete) {
          out.onCompleted();
        }
      } catch (RuntimeException ignored) {
        // already cancelled
      } finally {
        flushing.set(false);
      }
    });
  }

  void flush() {
    if (!flushing.compareAndSet(false, true)) {
      return;
    }
    try {
      while (!stopped && !queue.isEmpty() && out.isReady()) {
        DeliveryAck.Builder b = DeliveryAck.newBuilder();
        AckItem a;
        while (b.getAcksCount() < BATCH && (a = queue.poll()) != null) {
          size.decrementAndGet();
          b.addAcks(a);
        }
        if (b.getAcksCount() > 0) {
          out.onNext(b.build());
        }
      }
    } catch (RuntimeException e) {
      // The call is gone (cancelled or closed): nothing more can be sent on it.
      stop();
    } finally {
      flushing.set(false);
    }
  }
}
