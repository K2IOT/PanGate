package com.pangate.gateway.trace;

import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.ReadWriteSpan;
import io.opentelemetry.sdk.trace.ReadableSpan;
import io.opentelemetry.sdk.trace.SpanProcessor;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Exports ended spans in batches without ever parking the caller: spans end on gw-io threads (PushDelivery), and the
 * SDK's BatchSpanProcessor signals its worker through an ArrayBlockingQueue (a lock). Here the hand-off is a CAS
 * queue with a size bound (full = drop and count); a daemon thread "gw-otel" exports every {@code interval}.
 */
public final class LoopSafeSpanProcessor implements SpanProcessor {

  static final int MAX_BATCH = 512;

  private final SpanExporter exporter;
  private final int maxQueue;
  private final ConcurrentLinkedQueue<SpanData> queue = new ConcurrentLinkedQueue<>();
  private final AtomicInteger size = new AtomicInteger();
  private final AtomicLong dropped = new AtomicLong();
  private final Thread worker;
  private volatile boolean running = true;

  public LoopSafeSpanProcessor(SpanExporter exporter, int maxQueue, Duration interval) {
    this.exporter = exporter;
    this.maxQueue = maxQueue;
    this.worker = Thread.ofPlatform().daemon().name("gw-otel").start(() -> {
      while (running) {
        try {
          Thread.sleep(interval.toMillis());
        } catch (InterruptedException e) {
          break;
        }
        exportAll();
      }
    });
  }

  @Override
  public void onStart(Context parentContext, ReadWriteSpan span) {}

  @Override
  public boolean isStartRequired() {
    return false;
  }

  @Override
  public void onEnd(ReadableSpan span) {
    if (!span.getSpanContext().isSampled()) {
      return;
    }
    if (size.incrementAndGet() > maxQueue) {
      size.decrementAndGet();
      dropped.incrementAndGet();
      return;
    }
    queue.offer(span.toSpanData());
  }

  @Override
  public boolean isEndRequired() {
    return true;
  }

  public long dropped() {
    return dropped.get();
  }

  private void exportAll() {
    List<SpanData> batch = new ArrayList<>(MAX_BATCH);
    for (SpanData s; (s = queue.poll()) != null; ) {
      size.decrementAndGet();
      batch.add(s);
      if (batch.size() == MAX_BATCH) {
        exporter.export(new ArrayList<>(batch)).join(10, TimeUnit.SECONDS);
        batch.clear();
      }
    }
    if (!batch.isEmpty()) {
      exporter.export(batch).join(10, TimeUnit.SECONDS);
    }
  }

  @Override
  public CompletableResultCode forceFlush() {
    exportAll();
    return exporter.flush();
  }

  @Override
  public CompletableResultCode shutdown() {
    running = false;
    worker.interrupt();
    try {
      worker.join(5_000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    exportAll();
    return exporter.shutdown();
  }
}
