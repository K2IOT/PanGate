package com.pangate.pushapi.grpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.pangate.contracts.testkit.Fixtures;
import com.pangate.contracts.testkit.PushPathScenario;
import com.pangate.contracts.testkit.PushTopics;
import com.pangate.contracts.v1.PushRecord;
import com.pangate.contracts.v1.PushRequest;
import com.pangate.contracts.v1.PushResponse;
import com.pangate.contracts.v1.PushServiceGrpc;
import com.pangate.pushapi.support.PushApiHarness;
import com.pangate.pushapi.support.TestCerts;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

class SendStreamIT {

  @RegisterExtension static PushPathScenario scenario = new PushPathScenario("c1");
  static final GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7.4.2"))
      .withExposedPorts(6379);
  @TempDir static Path certDir;
  static PushApiHarness plain;
  static PushApiHarness tls;
  static TestCerts certs;

  @BeforeAll
  static void boot() throws Exception {
    redis.start();
    try (Admin admin = Admin.create(Map.of("bootstrap.servers", scenario.bootstrapServers()))) {
      try {
        admin.createTopics(List.of(new NewTopic("gw.control", 8, (short) 1))).all().get();
      } catch (Exception e) {
        // exists
      }
    }
    String redisUri = "redis://" + redis.getHost() + ":" + redis.getMappedPort(6379);
    plain = new PushApiHarness(scenario.bootstrapServers(), redisUri);
    certs = new TestCerts(certDir);
    TestCerts.Leaf server = certs.leaf("push-api", "spiffe://pangate/cell-1/push-api");
    tls = new PushApiHarness(scenario.bootstrapServers(), redisUri, "--push-api.grpc.plaintext=false",
        "--push-api.grpc.tls.cert=" + server.cert(), "--push-api.grpc.tls.key=" + server.key(),
        "--push-api.grpc.tls.trust=" + certs.ca());
  }

  @AfterAll
  static void shutdown() {
    if (tls != null) {
      tls.close();
    }
    if (plain != null) {
      plain.close();
    }
    redis.stop();
  }

  /** A client stream; responses keyed by request_id. */
  static final class Client implements AutoCloseable {
    final ManagedChannel channel;
    final Map<String, PushResponse> responses = new ConcurrentHashMap<>();
    final List<String> order = java.util.Collections.synchronizedList(new ArrayList<>());
    final CompletableFuture<io.grpc.Status> ended = new CompletableFuture<>();
    final StreamObserver<PushRequest> out;

    Client(ManagedChannel channel) {
      this.channel = channel;
      out = PushServiceGrpc.newStub(channel).sendStream(new StreamObserver<>() {
        @Override public void onNext(PushResponse r) {
          responses.put(r.getRequestId(), r);
          order.add(r.getRequestId());
        }
        @Override public void onError(Throwable t) { ended.complete(io.grpc.Status.fromThrowable(t)); }
        @Override public void onCompleted() { ended.complete(io.grpc.Status.OK); }
      });
    }

    @Override
    public void close() {
      channel.shutdownNow();
    }
  }

  static Client plainClient() {
    return new Client(NettyChannelBuilder.forAddress("127.0.0.1", plain.grpc).usePlaintext().build());
  }

  static PushRequest message(String id, String principal) {
    return Fixtures.request(PushRequest.Kind.KIND_MESSAGE).toBuilder().setRequestId(id).setPrincipalId(principal)
        .build();
  }

  @Test
  void gatewayKickIsOkAndProducesControl() throws Exception {
    try (Client c = plainClient()) {
      c.out.onNext(Fixtures.kickRequest010());
      await().atMost(Duration.ofSeconds(10)).until(() -> c.responses.containsKey(Fixtures.kickRequest010()
          .getRequestId()));
      PushResponse r = c.responses.get(Fixtures.kickRequest010().getRequestId());
      assertThat(r.getOk()).isTrue();
      assertThat(r.getCode()).isEqualTo(PushResponse.Code.CODE_OK);
      PushRecord rec = scenario.awaitRequest(r.getMsgId());
      assertThat(rec.getKind()).isEqualTo(PushRecord.Kind.KIND_CONTROL);
      assertThat(rec.getControl().hasKick()).isTrue();
    }
  }

  @Test
  void responsesMatchRequestIdsOutOfOrder() throws Exception {
    try (Client c = plainClient()) {
      String run = UUID.randomUUID().toString().substring(0, 6);
      for (int i = 0; i < 200; i++) {
        c.out.onNext(message(run + "-" + i, "t1:g" + run + "-" + (i % 20)));
      }
      await().atMost(Duration.ofSeconds(30)).until(() -> c.responses.size() == 200);
      assertThat(c.responses.values()).allMatch(r -> r.getCode() == PushResponse.Code.CODE_OK);
      assertThat(c.order).doesNotHaveDuplicates();
    }
  }

  @Test
  void samePrincipalOnOneStreamKeepsOrder() throws Exception {
    try (Client c = plainClient()) {
      String p = "t1:o-" + UUID.randomUUID().toString().substring(0, 6);
      for (int i = 0; i < 30; i++) {
        c.out.onNext(message("o" + i, p));
      }
      await().atMost(Duration.ofSeconds(20)).until(() -> c.responses.size() == 30);
      for (int i = 0; i < 30; i++) {
        assertThat(c.responses.get("o" + i).getSeq()).isEqualTo(i + 1);
      }
      scenario.awaitRequest(c.responses.get("o29").getMsgId());
      List<Long> seqs = new ArrayList<>();
      for (var rec : scenario.drain(PushTopics.requests("c1").name())) {
        PushRecord r = PushRecord.parseFrom(rec.value());
        if (r.getPrincipalId().equals(p)) {
          seqs.add(r.getFrame().getSeq());
        }
      }
      assertThat(seqs).isSorted().hasSize(30);
    }
  }

  @Test
  void disallowedSpiffeIsPermissionDenied() throws Exception {
    TestCerts.Leaf gw = certs.leaf("gateway", "spiffe://pangate/cell-1/gateway");
    TestCerts.Leaf other = certs.leaf("other", "spiffe://pangate/cell-1/other");
    try (Client ok = new Client(NettyChannelBuilder.forAddress("localhost", tls.grpc).sslContext(GrpcSslContexts
        .forClient().keyManager(gw.cert().toFile(), gw.key().toFile()).trustManager(certs.ca().toFile()).build())
        .build());
        Client denied = new Client(NettyChannelBuilder.forAddress("localhost", tls.grpc).sslContext(GrpcSslContexts
            .forClient().keyManager(other.cert().toFile(), other.key().toFile()).trustManager(certs.ca().toFile())
            .build()).build())) {
      ok.out.onNext(Fixtures.kickRequest010());
      await().atMost(Duration.ofSeconds(10)).until(() -> ok.responses.size() == 1);
      assertThat(ok.responses.values()).allMatch(PushResponse::getOk);
      denied.out.onNext(Fixtures.kickRequest010());
      assertThat(denied.ended.get(10, TimeUnit.SECONDS).getCode()).isEqualTo(io.grpc.Status.Code.PERMISSION_DENIED);
      assertThat(denied.responses).isEmpty();
    }
  }
}
