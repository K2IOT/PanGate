package com.pangate.devtools.push;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.contracts.testkit.PushHeaders;
import com.pangate.contracts.testkit.PushKeys;
import com.pangate.contracts.testkit.PushRecordRules;
import com.pangate.contracts.v1.DeliveryClass;
import com.pangate.contracts.v1.PushRecord;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

class PushProducerTest {

  final MockProducer<String, byte[]> mock = new MockProducer<>(true, new StringSerializer(), new ByteArraySerializer());
  final List<String> incr = new ArrayList<>();
  final PushProducer push = new PushProducer(mock, "c1", key -> {
    incr.add(key);
    return 41 + incr.size();
  }, () -> 1_000_000L);

  static final byte[] PAYLOAD = "{\"hi\":1}".getBytes(StandardCharsets.UTF_8);

  PushRecord sent(int i) throws Exception {
    return PushRecord.parseFrom(mock.history().get(i).value());
  }

  static void assertConventions(ProducerRecord<String, byte[]> pr) {
    ConsumerRecord<String, byte[]> cr = new ConsumerRecord<>(pr.topic(), 0, 0, pr.key(), pr.value());
    pr.headers().forEach(h -> cr.headers().add(h));
    PushHeaders.assertConventions(cr, "PushRecord");
  }

  @Test
  void principalMessageGetsSeqFromIncrAndPassesTheRules() throws Exception {
    PushRecord r = push.message("t1:u1", null, DeliveryClass.DELIVERY_CLASS_RELIABLE, PAYLOAD, true);
    assertThat(incr).containsExactly("t1:u1");
    assertThat(r.getFrame().getSeq()).isEqualTo(42);
    assertThat(r.getFrame().getSeqEpoch()).isZero();
    assertThat(r.getMsgId()).hasSize(26);
    assertThat(r.getExpiresAtMs()).isEqualTo(1_000_000L + 86_400_000L);
    PushRecordRules.assertValid(r);
    assertThat(sent(0)).isEqualTo(r);
  }

  @Test
  void ephemeralMessageTakesNoSeq() {
    PushRecord r = push.message("t1:u1", null, DeliveryClass.DELIVERY_CLASS_EPHEMERAL, PAYLOAD, false);
    assertThat(incr).isEmpty();
    assertThat(r.getFrame().getSeq()).isZero();
    PushRecordRules.assertValid(r);
  }

  @Test
  void deviceMessageHasNoSeq() {
    PushRecord r = push.message("t1:u1", "d2", DeliveryClass.DELIVERY_CLASS_RELIABLE, PAYLOAD, false);
    assertThat(incr).isEmpty();
    assertThat(r.getTargetCase()).isEqualTo(PushRecord.TargetCase.DEVICE);
    assertThat(r.getDevice().getDeviceId()).isEqualTo("d2");
    assertThat(r.getFrame().getSeq()).isZero();
    PushRecordRules.assertValid(r);
  }

  @Test
  void broadcastHasFrameTopic() {
    PushRecord r = push.broadcast("t1:news", DeliveryClass.DELIVERY_CLASS_EPHEMERAL, PAYLOAD);
    assertThat(r.getFrame().getTopic()).isEqualTo("t1:news");
    assertThat(r.getTopic()).isEqualTo("t1:news");
    PushRecordRules.assertValid(r);
    PushRecord reliable = push.broadcast("t1:news", DeliveryClass.DELIVERY_CLASS_RELIABLE, PAYLOAD);
    assertThat(reliable.getFrame().getTopicSeq()).isPositive();
    PushRecordRules.assertValid(reliable);
  }

  @Test
  void keyAndHeadersMatchTheTestkit() {
    PushRecord r = push.message("t1:u1", null, DeliveryClass.DELIVERY_CLASS_RELIABLE, PAYLOAD, false);
    ProducerRecord<String, byte[]> pr = mock.history().get(0);
    assertThat(pr.topic()).isEqualTo("c1.push.requests");
    assertThat(pr.key()).isEqualTo(PushKeys.of(r));
    assertConventions(pr);
  }
}
