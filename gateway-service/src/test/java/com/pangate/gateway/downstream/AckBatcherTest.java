package com.pangate.gateway.downstream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.pangate.contracts.v1.AckItem;
import com.pangate.contracts.v1.DeliveryAck;
import com.pangate.gateway.metrics.GatewayMetrics;
import io.grpc.stub.ServerCallStreamObserver;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class AckBatcherTest {

  /** Records onNext; readiness is controlled by the test. */
  static final class FakeObserver extends ServerCallStreamObserver<DeliveryAck> {
    final List<DeliveryAck> sent = new CopyOnWriteArrayList<>();
    volatile boolean ready = true;
    volatile Runnable onReady;

    @Override public boolean isReady() { return ready; }
    @Override public void setOnReadyHandler(Runnable r) { onReady = r; }
    @Override public boolean isCancelled() { return false; }
    @Override public void setOnCancelHandler(Runnable r) { }
    @Override public void setCompression(String c) { }
    @Override public void disableAutoInboundFlowControl() { }
    @Override public void request(int n) { }
    @Override public void setMessageCompression(boolean e) { }
    @Override public void onNext(DeliveryAck v) { sent.add(v); }
    @Override public void onError(Throwable t) { }
    @Override public void onCompleted() { }

    int items() {
      return sent.stream().mapToInt(DeliveryAck::getAcksCount).sum();
    }
  }

  private final ScheduledExecutorService exec = Executors.newScheduledThreadPool(1);
  private final SimpleMeterRegistry meters = new SimpleMeterRegistry();
  private final FakeObserver out = new FakeObserver();
  private final AckBatcher batcher = new AckBatcher(out, exec, new GatewayMetrics(meters));

  @AfterEach
  void stop() {
    batcher.stop();
    exec.shutdownNow();
  }

  private static AckItem item(long id) {
    return AckItem.newBuilder().setDeliveryId(id).setStatus(AckItem.Status.STATUS_ACKED).build();
  }

  @Test
  void acksAreBatchedEvery2ms() {
    batcher.start();
    batcher.offer(item(1));
    batcher.offer(item(2));
    batcher.offer(item(3));
    await().atMost(Duration.ofMillis(500)).until(() -> out.items() == 3);
    assertThat(out.sent).hasSize(1);
    assertThat(out.sent.get(0).getAcksList()).extracting(AckItem::getDeliveryId).containsExactly(1L, 2L, 3L);
  }

  @Test
  void queueOf256FlushesImmediatelyInChunksOf256() throws Exception {
    // not started: no periodic flush, only the size trigger. Hold the executor so all 600 are queued first.
    java.util.concurrent.CountDownLatch gate = new java.util.concurrent.CountDownLatch(1);
    exec.execute(() -> {
      try {
        gate.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });
    for (int i = 0; i < 600; i++) {
      batcher.offer(item(i));
    }
    gate.countDown();
    await().atMost(Duration.ofSeconds(2)).until(() -> out.items() == 600);
    assertThat(out.sent).extracting(DeliveryAck::getAcksCount).containsExactly(256, 256, 88);
  }

  @Test
  void belowThresholdWaitsForPeriodicFlush() throws Exception {
    for (int i = 0; i < 255; i++) {
      batcher.offer(item(i));
    }
    Thread.sleep(50);
    assertThat(out.sent).isEmpty();
    batcher.start();
    await().atMost(Duration.ofSeconds(2)).until(() -> out.items() == 255);
  }

  @Test
  void notReadyHoldsAcksUntilOnReady() throws Exception {
    out.ready = false;
    batcher.start();
    batcher.offer(item(1));
    Thread.sleep(50);
    assertThat(out.sent).isEmpty();
    out.ready = true;
    out.onReady.run();
    await().atMost(Duration.ofMillis(500)).until(() -> out.items() == 1);
  }

  @Test
  void deadStreamDropsAcksSilently() throws Exception {
    batcher.start();
    StreamRef ref = new StreamRef(batcher);
    assertThat(ref.alive()).isTrue();
    ref.markDead();
    ref.ack(1, AckItem.Status.STATUS_ACKED);
    Thread.sleep(30);
    assertThat(out.sent).isEmpty();
  }

  @Test
  void countsAcksByStatus() {
    StreamRef ref = new StreamRef(batcher);
    ref.ack(1, AckItem.Status.STATUS_ACKED);
    ref.ack(2, AckItem.Status.STATUS_ACKED);
    ref.ack(3, AckItem.Status.STATUS_NACK_TIMEOUT);
    assertThat(meters.counter("gw.delivery.acks", "status", "acked").count()).isEqualTo(2);
    assertThat(meters.counter("gw.delivery.acks", "status", "nack_timeout").count()).isEqualTo(1);
  }
}
