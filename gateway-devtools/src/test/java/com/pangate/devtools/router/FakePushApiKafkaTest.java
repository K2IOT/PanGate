package com.pangate.devtools.router;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.pangate.contracts.testkit.PushHeaders;
import com.pangate.contracts.v1.ControlOp;
import com.pangate.contracts.v1.Kick;
import com.pangate.contracts.v1.PushRecord;
import com.pangate.contracts.v1.PushRequest;
import com.pangate.contracts.v1.PushResponse;
import com.pangate.contracts.v1.PushServiceGrpc;
import com.pangate.devtools.push.PushProducer;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class FakePushApiKafkaTest {

  final MockProducer<String, byte[]> mock = new MockProducer<>(true, new StringSerializer(), new ByteArraySerializer());
  final FakePushApi api = FakePushApi.start(0, new PushProducer(mock, "c1", k -> 1, System::currentTimeMillis));
  final ManagedChannel channel = NettyChannelBuilder.forAddress("localhost", api.port()).usePlaintext().build();

  @AfterEach
  void stop() {
    channel.shutdownNow();
    api.close();
  }

  @Test
  void kickBecomesAControlRecordOnKafkaAndIsAnsweredOk() throws Exception {
    List<PushResponse> responses = new CopyOnWriteArrayList<>();
    StreamObserver<PushRequest> out = PushServiceGrpc.newStub(channel).sendStream(new StreamObserver<>() {
      @Override public void onNext(PushResponse r) { responses.add(r); }
      @Override public void onError(Throwable t) { }
      @Override public void onCompleted() { }
    });
    ControlOp kick = ControlOp.newBuilder().setKick(Kick.newBuilder().setNewConnId("NEW").setConnectedAtMs(5)).build();
    out.onNext(PushRequest.newBuilder().setRequestId("r1").setKind(PushRequest.Kind.KIND_CONTROL).setTenantId("t1")
        .setPrincipalId("t1:u1").setControl(kick).build());
    await().atMost(Duration.ofSeconds(5)).until(() -> !responses.isEmpty());
    assertThat(responses.get(0).getRequestId()).isEqualTo("r1");
    assertThat(responses.get(0).getOk()).isTrue();
    assertThat(responses.get(0).getCode()).isEqualTo(PushResponse.Code.CODE_OK);
    assertThat(mock.history()).hasSize(1);
    ProducerRecord<String, byte[]> pr = mock.history().get(0);
    assertThat(pr.topic()).isEqualTo("c1.push.requests");
    assertThat(pr.key()).isEqualTo("t1:u1");
    PushRecord r = PushRecord.parseFrom(pr.value());
    assertThat(r.getKind()).isEqualTo(PushRecord.Kind.KIND_CONTROL);
    assertThat(r.getControl()).isEqualTo(kick);
    assertThat(responses.get(0).getMsgId()).isEqualTo(r.getMsgId());
    ConsumerRecord<String, byte[]> cr = new ConsumerRecord<>(pr.topic(), 0, 0, pr.key(), pr.value());
    pr.headers().forEach(h -> cr.headers().add(h));
    PushHeaders.assertConventions(cr, "PushRecord");
    out.onCompleted();
  }
}
