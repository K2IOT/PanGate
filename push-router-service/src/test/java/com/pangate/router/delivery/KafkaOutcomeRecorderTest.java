package com.pangate.router.delivery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.pangate.contracts.testkit.Fixtures;
import com.pangate.contracts.testkit.PushKeys;
import com.pangate.contracts.v1.AckItem.Status;
import com.pangate.contracts.v1.DeliveredRecord;
import com.pangate.contracts.v1.OfflineRecord;
import com.pangate.contracts.v1.OfflineRecord.Reason;
import com.pangate.contracts.v1.PushRecord;
import com.pangate.router.registry.LiveConnection;
import com.pangate.starter.kafka.PangateHeaders;
import java.util.Optional;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

class KafkaOutcomeRecorderTest {

  final MockProducer<String, byte[]> producer =
      new MockProducer<>(true, new StringSerializer(), new ByteArraySerializer());
  final KafkaOutcomeRecorder rec = new KafkaOutcomeRecorder(producer, "c1", () -> 42L);

  @Test
  void offlineGoesToTheOfflineTopicKeyedLikePushKeys() throws Exception {
    PushRecord r = Fixtures.message("t1:u1");
    rec.offline(r, Reason.REASON_NO_SESSION, 0);
    var sent = producer.history().get(0);
    assertThat(sent.topic()).isEqualTo("c1.push.offline");
    OfflineRecord o = OfflineRecord.parseFrom(sent.value());
    assertThat(o).isEqualTo(Fixtures.offline(r, Reason.REASON_NO_SESSION, 0).toBuilder().setAtMs(42).build());
    assertThat(sent.key()).isEqualTo(PushKeys.of(o));
    assertThat(PangateHeaders.read(sent.headers(), PangateHeaders.TENANT_ID)).contains(r.getTenantId());
  }

  @Test
  void deliveredGoesToTheDeliveredTopicKeyedBySender() throws Exception {
    PushRecord r = Fixtures.deviceMessage("t1:u1", "d2");
    rec.delivered(r, new LiveConnection("C1", "n1", 1, Optional.of("d2")), Status.STATUS_ACKED);
    var sent = producer.history().get(0);
    assertThat(sent.topic()).isEqualTo("c1.push.delivered");
    DeliveredRecord d = DeliveredRecord.parseFrom(sent.value());
    assertThat(d.getMsgId()).isEqualTo(r.getMsgId());
    assertThat(d.getPrincipalId()).isEqualTo("t1:u1");
    assertThat(d.getDeviceId()).isEqualTo("d2");
    assertThat(d.getConnId()).isEqualTo("C1");
    assertThat(d.getStatus()).isEqualTo(Status.STATUS_ACKED);
    assertThat(d.getAtMs()).isEqualTo(42);
    assertThat(sent.key()).isEqualTo(PushKeys.of(d));
  }

  @Test
  void aFailedSendThrowsSoTheRecordIsRetried() {
    MockProducer<String, byte[]> failing = new MockProducer<>(false, new StringSerializer(), new ByteArraySerializer());
    var r2 = new KafkaOutcomeRecorder(failing, "c1", () -> 42L);
    Thread t = Thread.ofVirtual().start(() -> {
      while (failing.history().isEmpty()) Thread.onSpinWait();
      failing.errorNext(new RuntimeException("broker down"));
    });
    assertThatThrownBy(() -> r2.offline(Fixtures.message("t1:u1"), Reason.REASON_CLOSED, 3))
        .isInstanceOf(IllegalStateException.class);
  }
}
