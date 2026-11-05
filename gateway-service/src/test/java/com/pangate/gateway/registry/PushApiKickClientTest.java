package com.pangate.gateway.registry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.pangate.contracts.v1.PushRequest;
import com.pangate.contracts.v1.PushResponse;
import com.pangate.contracts.v1.PushServiceGrpc;
import com.pangate.gateway.metrics.GatewayMetrics;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class PushApiKickClientTest {

  private final ScheduledExecutorService sched = Executors.newSingleThreadScheduledExecutor();
  private final SimpleMeterRegistry meters = new SimpleMeterRegistry();
  private final List<PushRequest> received = new CopyOnWriteArrayList<>();
  private final AtomicInteger streams = new AtomicInteger();
  /** ok, fail, or error the whole stream. */
  private volatile String mode = "ok";
  private Server server;
  private PushApiKickClient client;

  private void start() throws Exception {
    String name = InProcessServerBuilder.generateName();
    server = InProcessServerBuilder.forName(name).addService(new PushServiceGrpc.PushServiceImplBase() {
      @Override
      public StreamObserver<PushRequest> sendStream(StreamObserver<PushResponse> out) {
        streams.incrementAndGet();
        return new StreamObserver<>() {
          @Override
          public void onNext(PushRequest r) {
            received.add(r);
            switch (mode) {
              case "error" -> out.onError(Status.UNAVAILABLE.asRuntimeException());
              case "fail" -> out.onNext(PushResponse.newBuilder().setRequestId(r.getRequestId()).setOk(false)
                  .setError("nope").build());
              default -> out.onNext(PushResponse.newBuilder().setRequestId(r.getRequestId()).setOk(true).build());
            }
          }

          @Override public void onError(Throwable t) { }
          @Override public void onCompleted() { out.onCompleted(); }
        };
      }
    }).build().start();
    client = new PushApiKickClient(InProcessChannelBuilder.forName(name).build(), sched,
        new GatewayMetrics(meters), Duration.ofSeconds(2));
  }

  @AfterEach
  void stop() {
    client.close();
    server.shutdownNow();
    sched.shutdownNow();
  }

  @Test
  void sendsControlKickAndCompletesOnOkResponse() throws Exception {
    start();
    assertThat(client.kickAsync("t1", "t1:u1", "NEWCONN", 1234).toCompletableFuture().get()).isTrue();
    assertThat(received).hasSize(1);
    PushRequest r = received.get(0);
    assertThat(r.getKind()).isEqualTo(PushRequest.Kind.KIND_CONTROL);
    assertThat(r.getTenantId()).isEqualTo("t1");
    assertThat(r.getPrincipalId()).isEqualTo("t1:u1");
    assertThat(r.getControl().getKick().getNewConnId()).isEqualTo("NEWCONN");
    assertThat(r.getControl().getKick().getConnectedAtMs()).isEqualTo(1234);
    assertThat(r.getRequestId()).isNotEmpty();
  }

  @Test
  void retriesTwiceThenCountsFailure() throws Exception {
    start();
    mode = "fail";
    client.kick("t1", "t1:u1", "C", 1);
    await().atMost(Duration.ofSeconds(3)).until(() -> meters.counter("gw.kick.failures").count() == 1);
    assertThat(received).hasSize(3);
  }

  @Test
  void reopensStreamAfterError() throws Exception {
    start();
    mode = "error";
    client.kick("t1", "t1:u1", "C", 1);
    await().atMost(Duration.ofSeconds(3)).until(() -> meters.counter("gw.kick.failures").count() == 1);
    assertThat(streams.get()).isEqualTo(3);
    mode = "ok";
    assertThat(client.kickAsync("t1", "t1:u1", "C", 2).toCompletableFuture().get()).isTrue();
    assertThat(streams.get()).isEqualTo(4);
  }

  @Test
  void kickReturnsAtOnceAndSendsFromTheScheduler() throws Exception {
    start();
    java.util.concurrent.CountDownLatch gate = new java.util.concurrent.CountDownLatch(1);
    sched.execute(() -> {
      try {
        gate.await();  // the scheduler is busy: a kick that sends on the caller's thread would still go out
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });
    client.kick("t1", "t1:u1", "C", 1);
    Thread.sleep(200);
    assertThat(received).isEmpty();
    gate.countDown();
    await().atMost(Duration.ofSeconds(3)).until(() -> received.size() == 1);
  }
}
