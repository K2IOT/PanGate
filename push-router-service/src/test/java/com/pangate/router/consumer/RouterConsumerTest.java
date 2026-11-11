package com.pangate.router.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.pangate.contracts.testkit.Fixtures;
import com.pangate.contracts.testkit.PushRecordRules;
import com.pangate.contracts.v1.DeliveryClass;
import com.pangate.contracts.v1.PushRecord;
import com.pangate.router.config.RouterMetrics;
import com.pangate.router.delivery.DeliveryResult;
import com.pangate.router.registry.RegistryUnavailableException;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.function.UnaryOperator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

class RouterConsumerTest {

  final SimpleMeterRegistry meters = new SimpleMeterRegistry();
  final List<String> ran = new ArrayList<>();
  final RouterConsumer consumer = new RouterConsumer(r -> {
    ran.add(r.getMsgId());
    return DeliveryResult.DELIVERED;
  }, new RouterMetrics(meters), "c1.push.requests", () -> Fixtures.FIXED_NOW_MS + 10);

  static ConsumerRecord<String, byte[]> rec(long offset, byte[] value) {
    return new ConsumerRecord<>("c1.push.requests", 3, offset, "t1:u1", value);
  }

  double count(String result) {
    var c = meters.find("router.records").tag("result", result).counter();
    return c == null ? 0 : c.count();
  }

  @Test
  void corruptRecordIsSkippedAndDoesNotBlockThePartition() {
    assertThatCode(() -> consumer.handle(rec(7, new byte[] {(byte) 0xff}))).doesNotThrowAnyException();
    PushRecord ok = Fixtures.message("t1:u1");
    consumer.handle(rec(8, ok.toByteArray()));
    assertThat(ran).containsExactly(ok.getMsgId());
    assertThat(count("invalid")).isEqualTo(1);
    assertThat(count("delivered")).isEqualTo(1);
  }

  @Test
  void aRecordBreakingTheRulesIsSkipped() {
    PushRecord bad = Fixtures.message("t1:u1").toBuilder().setTenantId("").build();
    consumer.handle(rec(1, bad.toByteArray()));
    consumer.handle(rec(2, null));
    assertThat(ran).isEmpty();
    assertThat(count("invalid")).isEqualTo(2);
  }

  @Test
  void aTransientFailurePropagatesSoTheRecordIsRetried() {
    RouterConsumer failing = new RouterConsumer(r -> {
      throw new RegistryUnavailableException("ab", new RuntimeException("timeout"));
    }, new RouterMetrics(meters), "c1.push.requests", () -> 0L);
    assertThatThrownBy(() -> failing.handle(rec(1, Fixtures.message("t1:u1").toByteArray())))
        .isInstanceOf(RegistryUnavailableException.class);
    assertThat(count("invalid")).isZero();
  }

  /** The router's own check must agree with the testkit's PushRecordRules (push contracts §1.1). */
  @Test
  void ruleCheckAgreesWithTheTestkit() {
    List<PushRecord> valid = List.of(Fixtures.message("t1:u1"), Fixtures.ephemeral("t1:u1"),
        Fixtures.deviceMessage("t1:u1", "d2"), Fixtures.broadcast("t1:news"),
        Fixtures.control("t1:u1", Fixtures.subscribe("t1:news")));
    List<UnaryOperator<PushRecord.Builder>> breaks = List.of(
        b -> b.setMsgId(""), b -> b.setTenantId(""), b -> b.setExpiresAtMs(b.getAcceptedAtMs() - 1),
        b -> b.setTenantId("t2"), b -> b.clearFrame(), b -> b.setReceipt(true).setClass_(DeliveryClass.DELIVERY_CLASS_EPHEMERAL),
        b -> b.setFrame(b.getFrame().toBuilder().setMsgId("other")), b -> b.clearKind(), b -> b.clearTarget(),
        b -> b.setClass_(DeliveryClass.DELIVERY_CLASS_ORDERED), b -> b.setFrame(b.getFrame().toBuilder().setSeq(0)));
    for (PushRecord v : valid) {
      assertThat(PushRecordCheck.violation(v)).as(v.getKind().name()).isEmpty();
      for (UnaryOperator<PushRecord.Builder> br : breaks) {
        PushRecord m = br.apply(v.toBuilder()).build();
        boolean testkitRejects;
        try {
          PushRecordRules.assertValid(m);
          testkitRejects = false;
        } catch (AssertionError e) {
          testkitRejects = true;
        }
        assertThat(PushRecordCheck.violation(m).isPresent()).as(m.toString()).isEqualTo(testkitRejects);
      }
    }
  }
}
