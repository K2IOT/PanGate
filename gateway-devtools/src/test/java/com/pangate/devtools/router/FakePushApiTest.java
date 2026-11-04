package com.pangate.devtools.router;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.pangate.contracts.v1.ControlOp;
import com.pangate.contracts.v1.Kick;
import com.pangate.contracts.v1.PushItem;
import com.pangate.contracts.v1.PushRequest;
import com.pangate.contracts.v1.PushResponse;
import com.pangate.contracts.v1.PushServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class FakePushApiTest {

  private final FakePushStreamServer gw1;
  private final FakePushStreamServer gw2;
  private final FakePushApi api;
  private final ManagedChannel channel;

  FakePushApiTest() throws Exception {
    gw1 = new FakePushStreamServer();
    gw2 = new FakePushStreamServer();
    api = FakePushApi.start(0, List.of(gw1.target(), gw2.target()));
    channel = NettyChannelBuilder.forAddress("localhost", api.port()).usePlaintext().build();
  }

  @AfterEach
  void stop() {
    channel.shutdownNow();
    api.close();
    gw1.close();
    gw2.close();
  }

  @Test
  void kickRequestBecomesControlItemOnEveryRouterAndIsAcked() {
    List<PushResponse> responses = new CopyOnWriteArrayList<>();
    StreamObserver<PushRequest> out = PushServiceGrpc.newStub(channel).sendStream(new StreamObserver<>() {
      @Override public void onNext(PushResponse r) { responses.add(r); }
      @Override public void onError(Throwable t) { }
      @Override public void onCompleted() { }
    });
    ControlOp kick = ControlOp.newBuilder().setKick(Kick.newBuilder().setNewConnId("NEW").setConnectedAtMs(5)).build();
    out.onNext(PushRequest.newBuilder().setRequestId("r1").setKind(PushRequest.Kind.KIND_CONTROL).setTenantId("t1")
        .setPrincipalId("t1:u1").setControl(kick).build());
    await().atMost(Duration.ofSeconds(5)).until(() -> !responses.isEmpty() && !gw1.batches.isEmpty()
        && !gw2.batches.isEmpty());
    assertThat(responses.get(0).getRequestId()).isEqualTo("r1");
    assertThat(responses.get(0).getOk()).isTrue();
    for (FakePushStreamServer gw : List.of(gw1, gw2)) {
      PushItem item = gw.batches.get(0).getItems(0);
      assertThat(item.getKind()).isEqualTo(PushItem.Kind.KIND_CONTROL);
      assertThat(item.getTenantId()).isEqualTo("t1");
      assertThat(item.getPrincipalId()).isEqualTo("t1:u1");
      assertThat(item.getControl()).isEqualTo(kick);
    }
    assertThat(api.received()).hasSize(1);
    out.onCompleted();
  }

  @Test
  void healthServing() {
    HealthCheckResponse r = HealthGrpc.newBlockingStub(channel).check(HealthCheckRequest.getDefaultInstance());
    assertThat(r.getStatus()).isEqualTo(HealthCheckResponse.ServingStatus.SERVING);
  }
}
