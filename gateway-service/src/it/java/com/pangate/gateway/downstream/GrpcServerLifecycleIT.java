package com.pangate.gateway.downstream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.pangate.contracts.v1.AckItem;
import com.pangate.contracts.v1.DeliveryAck;
import com.pangate.contracts.v1.PushBatch;
import com.pangate.contracts.v1.PushStreamGrpc;
import com.pangate.gateway.config.GatewayProperties;
import com.pangate.gateway.metrics.GatewayMetrics;
import com.pangate.gateway.server.NodeState;
import com.pangate.gateway.test.TestPki;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class GrpcServerLifecycleIT {

  private static final String ROUTER = "spiffe://pangate/cell-1/push-router";

  @TempDir Path dir;
  private final ScheduledExecutorService exec = Executors.newScheduledThreadPool(2);
  private GrpcServerLifecycle server;
  private ManagedChannel channel;

  @AfterEach
  void stop() {
    if (channel != null) {
      channel.shutdownNow();
    }
    if (server != null && server.isRunning()) {
      server.stop();
    }
    exec.shutdownNow();
  }

  private void start(boolean plaintext, TestPki.Issued cert, Path trust) {
    PushStreamService svc = new PushStreamService((b, r) -> r.ack(1, AckItem.Status.STATUS_ACKED), new NodeState(),
        exec, new GatewayMetrics(new SimpleMeterRegistry()));
    GatewayProperties.Grpc cfg = new GatewayProperties.Grpc(0, plaintext, List.of(ROUTER),
        new GatewayProperties.GrpcTls(cert == null ? null : cert.certFile(), cert == null ? null : cert.keyFile(),
            trust));
    server = new GrpcServerLifecycle(cfg, svc, exec);
    server.start();
  }

  private record Result(List<DeliveryAck> acks, AtomicReference<Throwable> error) {}

  private Result call() {
    Metadata h = new Metadata();
    h.put(PushVersionInterceptor.HEADER, "1");
    Result r = new Result(new CopyOnWriteArrayList<>(), new AtomicReference<>());
    StreamObserver<PushBatch> out = PushStreamGrpc.newStub(channel)
        .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(h)).stream(new StreamObserver<>() {
          @Override public void onNext(DeliveryAck v) { r.acks().add(v); }
          @Override public void onError(Throwable t) { r.error().set(t); }
          @Override public void onCompleted() { }
        });
    out.onNext(PushBatch.getDefaultInstance());
    return r;
  }

  private ManagedChannel mtls(TestPki pki, TestPki.Issued client) throws Exception {
    return NettyChannelBuilder.forAddress("localhost", server.port())
        .sslContext(GrpcSslContexts.forClient().trustManager(pki.caFile.toFile())
            .keyManager(client.certFile().toFile(), client.keyFile().toFile()).build())
        .build();
  }

  @Test
  void mtlsWithAllowedClientCertStreams() throws Exception {
    TestPki pki = new TestPki(dir);
    start(false, pki.server(), pki.caFile);
    channel = mtls(pki, pki.client("router", ROUTER));
    Result r = call();
    await().atMost(Duration.ofSeconds(5)).until(() -> !r.acks().isEmpty() || r.error().get() != null);
    assertThat(r.error().get()).isNull();
  }

  @Test
  void mtlsWithOtherSpiffeIdIsUnauthenticated() throws Exception {
    TestPki pki = new TestPki(dir);
    start(false, pki.server(), pki.caFile);
    channel = mtls(pki, pki.client("evil", "spiffe://pangate/cell-1/other"));
    Result r = call();
    await().atMost(Duration.ofSeconds(5)).until(() -> r.error().get() != null);
    assertThat(Status.fromThrowable(r.error().get()).getCode()).isEqualTo(Status.Code.UNAUTHENTICATED);
  }

  @Test
  void plaintextServesAndStopsWithin5s() {
    start(true, null, null);
    channel = NettyChannelBuilder.forAddress("127.0.0.1", server.port()).usePlaintext().build();
    Result r = call();
    await().atMost(Duration.ofSeconds(5)).until(() -> !r.acks().isEmpty());
    long t0 = System.nanoTime();
    server.stop();
    assertThat(Duration.ofNanos(System.nanoTime() - t0)).isLessThan(Duration.ofSeconds(6));
    assertThat(server.isRunning()).isFalse();
  }

  @Test
  void portInUseIsListenerBindException() throws Exception {
    try (java.net.ServerSocket taken = new java.net.ServerSocket(0)) {
      PushStreamService svc = new PushStreamService((b, r) -> { }, new NodeState(), exec,
          new GatewayMetrics(new SimpleMeterRegistry()));
      GrpcServerLifecycle s = new GrpcServerLifecycle(new GatewayProperties.Grpc(taken.getLocalPort(), true,
          List.of(), new GatewayProperties.GrpcTls(null, null, null)), svc, exec);
      org.assertj.core.api.Assertions.assertThatThrownBy(s::start)
          .isInstanceOfSatisfying(com.pangate.gateway.diagnostics.ListenerBindException.class,
              e -> assertThat(e.envVar()).isEqualTo("GATEWAY_GRPC_PORT"));
    }
  }
}
