package com.pangate.gateway.registry;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.contracts.v1.PushRequest;
import com.pangate.contracts.v1.PushResponse;
import com.pangate.contracts.v1.PushServiceGrpc;
import com.pangate.gateway.config.GatewayProperties;
import com.pangate.gateway.metrics.GatewayMetrics;
import com.pangate.gateway.test.TestPki;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.file.Path;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Production KICK path: mTLS to a Push API whose certificate is a SPIFFE SVID (URI SAN only, no DNS name). */
class PushApiKickClientIT {

  @TempDir Path dir;
  private final ScheduledExecutorService sched = Executors.newSingleThreadScheduledExecutor();
  private Server server;
  private PushApiKickClient client;

  @AfterEach
  void stop() {
    if (client != null) {
      client.close();
    }
    if (server != null) {
      server.shutdownNow();
    }
    sched.shutdownNow();
  }

  private void startPushApi(TestPki pki, TestPki.Issued cert) throws Exception {
    server = NettyServerBuilder.forPort(0)
        .sslContext(GrpcSslContexts.forServer(cert.certFile().toFile(), cert.keyFile().toFile())
            .trustManager(pki.caFile.toFile()).clientAuth(ClientAuth.REQUIRE).build())
        .addService(new PushServiceGrpc.PushServiceImplBase() {
          @Override
          public StreamObserver<PushRequest> sendStream(StreamObserver<PushResponse> out) {
            return new StreamObserver<>() {
              @Override
              public void onNext(PushRequest r) {
                out.onNext(PushResponse.newBuilder().setRequestId(r.getRequestId()).setOk(true).build());
              }

              @Override public void onError(Throwable t) { }
              @Override public void onCompleted() { out.onCompleted(); }
            };
          }
        }).build().start();
  }

  private boolean kick(TestPki pki, TestPki.Issued gatewayCert) throws Exception {
    client = PushApiKickClient.create(new GatewayProperties.PushApi(true, "localhost:" + server.getPort(), false),
        new GatewayProperties.GrpcTls(gatewayCert.certFile(), gatewayCert.keyFile(), pki.caFile), sched,
        new GatewayMetrics(new SimpleMeterRegistry()));
    return client.kickAsync("t1", "t1:u1", "C", 1).toCompletableFuture().get(10, TimeUnit.SECONDS);
  }

  @Test
  void mtlsToSpiffeSvidWithoutDnsNameSucceeds() throws Exception {
    TestPki pki = new TestPki(dir);
    startPushApi(pki, pki.client("push-api", "spiffe://pangate/cell-1/push-api"));
    assertThat(kick(pki, pki.client("gateway", "spiffe://pangate/cell-1/gateway"))).isTrue();
  }

  @Test
  void serverCertFromAnotherCaIsRejected() throws Exception {
    TestPki pki = new TestPki(dir);
    TestPki other = new TestPki(java.nio.file.Files.createTempDirectory(dir, "other"));
    startPushApi(pki, other.client("push-api", "spiffe://pangate/cell-1/push-api"));
    assertThat(kick(pki, pki.client("gateway", "spiffe://pangate/cell-1/gateway"))).isFalse();
  }
}
