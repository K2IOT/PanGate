package com.pangate.gateway.trace;

import static org.assertj.core.api.Assertions.assertThat;

import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;

/** Spans end on gw-io threads (PushDelivery): the batch processor's queue must never park them (BlockHound). */
class BatchExportOnEventLoopTest {

  @Test
  void endingSpansConcurrentlyOnEventLoopsNeverBlocks() throws Exception {
    InMemorySpanExporter exporter = InMemorySpanExporter.create();
    SdkTracerProvider provider = SdkTracerProvider.builder()
        .addSpanProcessor(new LoopSafeSpanProcessor(exporter, 1 << 18, java.time.Duration.ofMillis(50))).build();
    Tracer tracer = provider.get("test");
    io.opentelemetry.context.Context.current();  // context storage is set up at start-up, off the loops (EdgeConfig)
    List<Throwable> errors = new CopyOnWriteArrayList<>();
    java.util.concurrent.atomic.AtomicBoolean go = new java.util.concurrent.atomic.AtomicBoolean();
    List<Thread> threads = new java.util.ArrayList<>();
    for (int t = 0; t < 8; t++) {
      threads.add(Thread.ofPlatform().name("gw-io-span-" + t).start(() -> {
        try {
          while (!go.get()) {
            // busy-wait: BlockHound flags park and onSpinWait on gw-io threads
          }
          for (int i = 0; i < 20_000; i++) {
            tracer.spanBuilder("gw.push.write").startSpan().end();
          }
        } catch (Throwable e) {
          errors.add(e);
        }
      }));
    }
    go.set(true);
    for (Thread t : threads) {
      t.join();
    }
    provider.forceFlush().join(5, java.util.concurrent.TimeUnit.SECONDS);
    errors.stream().findFirst().ifPresent(Throwable::printStackTrace);
    assertThat(errors).isEmpty();
    assertThat(exporter.getFinishedSpanItems()).hasSize(8 * 20_000);  // nothing lost below the queue bound
    provider.shutdown().join(5, java.util.concurrent.TimeUnit.SECONDS);
  }

  @Test
  void aFullQueueDropsInsteadOfBlocking() {
    InMemorySpanExporter exporter = InMemorySpanExporter.create();
    LoopSafeSpanProcessor p = new LoopSafeSpanProcessor(exporter, 10, java.time.Duration.ofHours(1));
    SdkTracerProvider provider = SdkTracerProvider.builder().addSpanProcessor(p).build();
    for (int i = 0; i < 25; i++) {
      provider.get("t").spanBuilder("s").startSpan().end();
    }
    assertThat(p.dropped()).isEqualTo(15);
    provider.forceFlush().join(5, java.util.concurrent.TimeUnit.SECONDS);
    assertThat(exporter.getFinishedSpanItems()).hasSize(10);
    provider.shutdown().join(5, java.util.concurrent.TimeUnit.SECONDS);
  }
}
