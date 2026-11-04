package com.pangate.gateway.downstream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.pangate.contracts.v1.AckItem;
import com.pangate.contracts.v1.DeliveryAck;
import com.pangate.contracts.v1.PushBatch;
import com.pangate.contracts.v1.PushStreamGrpc;
import com.pangate.gateway.metrics.GatewayMetrics;
import com.pangate.gateway.server.NodeState;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.ServerInterceptors;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class PushStreamServiceTest {

  private final ScheduledExecutorService exec = Executors.newScheduledThreadPool(2);
  private final NodeState node = new NodeState();
  private final SimpleMeterRegistry meters = new SimpleMeterRegistry();
  private final List<PushBatch> dispatched = new CopyOnWriteArrayList<>();
  private final AtomicReference<StreamRef> lastRef = new AtomicReference<>();
  private volatile BiConsumer<PushBatch, StreamRef> onBatch = (b, r) -> { };
  private PushStreamService service;
  private ManagedChannel channel;
  private io.grpc.Server server;

  private void start() throws Exception {
    service = new PushStreamService((b, r) -> {
      dispatched.add(b);
      lastRef.set(r);
      onBatch.accept(b, r);
    }, node, exec, new GatewayMetrics(meters));
    String name = InProcessServerBuilder.generateName();
    server = InProcessServerBuilder.forName(name).executor(exec)
        .addService(ServerInterceptors.intercept(service, new PushVersionInterceptor())).build().start();
    channel = InProcessChannelBuilder.forName(name).build();
  }

  @AfterEach
  void stop() {
    if (channel != null) {
      channel.shutdownNow();
      server.shutdownNow();
    }
    exec.shutdownNow();
  }

  private PushStreamGrpc.PushStreamStub stub(String version) {
    Metadata h = new Metadata();
    if (version != null) {
      h.put(PushVersionInterceptor.HEADER, version);
    }
    return PushStreamGrpc.newStub(channel).withInterceptors(MetadataUtils.newAttachHeadersInterceptor(h));
  }

  static final class Client implements ClientResponseObserver<PushBatch, DeliveryAck> {
    final List<DeliveryAck> acks = new CopyOnWriteArrayList<>();
    final AtomicReference<Throwable> error = new AtomicReference<>();
    final CountDownLatch done = new CountDownLatch(1);
    volatile ClientCallStreamObserver<PushBatch> out;

    @Override public void beforeStart(ClientCallStreamObserver<PushBatch> s) { out = s; }
    @Override public void onNext(DeliveryAck v) { acks.add(v); }
    @Override public void onError(Throwable t) { error.set(t); done.countDown(); }
    @Override public void onCompleted() { done.countDown(); }
  }

  @Test
  void batchesAreDispatchedAndAcksStreamedBack() throws Exception {
    start();
    onBatch = (b, r) -> r.ack(7, AckItem.Status.STATUS_ACKED);
    Client c = new Client();
    StreamObserver<PushBatch> out = stub("1").stream(c);
    out.onNext(PushBatch.getDefaultInstance());
    await().atMost(Duration.ofSeconds(2)).until(() -> !c.acks.isEmpty());
    assertThat(c.acks.get(0).getAcks(0).getDeliveryId()).isEqualTo(7);
    assertThat(meters.get("gw.pushstream.active").gauge().value()).isEqualTo(1);
    out.onCompleted();
    assertThat(c.done.await(2, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
    await().atMost(Duration.ofSeconds(2)).until(() -> meters.get("gw.pushstream.active").gauge().value() == 0);
  }

  @Test
  void requestsOneMorePerDispatchedBatch() throws Exception {
    start();
    Client c = new Client();
    StreamObserver<PushBatch> out = stub("1").stream(c);
    for (int i = 0; i < 20; i++) {
      out.onNext(PushBatch.getDefaultInstance());
    }
    // initial request(8) alone would stop at 8 (see shedLevel4PausesRequestsAndTickResumes)
    await().atMost(Duration.ofSeconds(2)).until(() -> dispatched.size() == 20);
  }

  @Test
  void shedLevel4PausesRequestsAndTickResumes() throws Exception {
    start();
    node.shedLevel(4);
    Client c = new Client();
    StreamObserver<PushBatch> out = stub("1").stream(c);
    for (int i = 0; i < 12; i++) {
      out.onNext(PushBatch.getDefaultInstance());
    }
    await().atMost(Duration.ofSeconds(2)).until(() -> dispatched.size() == 8);
    Thread.sleep(100);
    service.resumePaused();
    assertThat(dispatched).hasSize(8);
    node.shedLevel(0);
    service.resumePaused();
    await().atMost(Duration.ofSeconds(2)).until(() -> dispatched.size() == 12);
  }

  @Test
  void missingOrWrongVersionHeaderFailsPrecondition() throws Exception {
    start();
    for (String v : new String[] {null, "2", "0"}) {
      Client c = new Client();
      stub(v).stream(c).onNext(PushBatch.getDefaultInstance());
      assertThat(c.done.await(2, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
      assertThat(Status.fromThrowable(c.error.get()).getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
    }
    assertThat(dispatched).isEmpty();
  }

  @Test
  void streamCancelMarksRefDead() throws Exception {
    start();
    Client c = new Client();
    StreamObserver<PushBatch> out = stub("1").stream(c);
    out.onNext(PushBatch.getDefaultInstance());
    await().atMost(Duration.ofSeconds(2)).until(() -> lastRef.get() != null);
    c.out.cancel("router gone", null);
    await().atMost(Duration.ofSeconds(2)).until(() -> !lastRef.get().alive());
    await().atMost(Duration.ofSeconds(2)).until(() -> meters.get("gw.pushstream.active").gauge().value() == 0);
    lastRef.get().ack(1, AckItem.Status.STATUS_ACKED);  // no exception, nothing sent
    assertThat(c.error.get()).isInstanceOf(StatusRuntimeException.class);
  }
}
