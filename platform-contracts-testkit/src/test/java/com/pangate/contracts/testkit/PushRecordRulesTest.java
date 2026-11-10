package com.pangate.contracts.testkit;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.pangate.contracts.v1.DeliveryClass;
import com.pangate.contracts.v1.PushRecord;
import org.junit.jupiter.api.Test;

/** The §1.1 rules as a reusable check for ③/④ tests (review Important 7). */
class PushRecordRulesTest {

  @Test
  void everyFixtureIsValid() {
    for (PushRecord r : new PushRecord[] {Fixtures.message("t1:u1"), Fixtures.deviceMessage("t1:u1", "d1"),
        Fixtures.ephemeral("t1:u1"), Fixtures.broadcast("t1:news"),
        Fixtures.control("t1:u1", Fixtures.subscribe("t1:news"))}) {
      assertThatCode(() -> PushRecordRules.assertValid(r)).as(r.getKind() + " " + r.getTargetCase())
          .doesNotThrowAnyException();
    }
  }

  @Test
  void withMsgIdKeepsTheFrameInStep() {
    PushRecord m = Fixtures.withMsgId(Fixtures.message("t1:u1"), "01JCOTHER00000000000000000");
    assertThatCode(() -> PushRecordRules.assertValid(m)).doesNotThrowAnyException();
    PushRecord stale = Fixtures.message("t1:u1").toBuilder().setMsgId("01JCOTHER00000000000000000").build();
    assertThatThrownBy(() -> PushRecordRules.assertValid(stale)).hasMessageContaining("frame.msg_id");
  }

  @Test
  void violationsAreNamed() {
    PushRecord b = Fixtures.broadcast("t1:news");
    assertThatThrownBy(() -> PushRecordRules.assertValid(b.toBuilder().setTopic("t1:other").build()))
        .hasMessageContaining("frame.topic");
    PushRecord e = Fixtures.ephemeral("t1:u1");
    assertThatThrownBy(() -> PushRecordRules.assertValid(e.toBuilder().setFrame(e.getFrame().toBuilder().setSeq(3))
        .build())).hasMessageContaining("seq");
    assertThatThrownBy(() -> PushRecordRules.assertValid(e.toBuilder().setReceipt(true).build()))
        .hasMessageContaining("receipt");
    PushRecord d = Fixtures.deviceMessage("t1:u1", "d1");
    assertThatThrownBy(() -> PushRecordRules.assertValid(d.toBuilder().setFrame(d.getFrame().toBuilder().setSeq(4))
        .build())).hasMessageContaining("device");
    assertThatThrownBy(() -> PushRecordRules.assertValid(d.toBuilder()
        .setClass_(DeliveryClass.DELIVERY_CLASS_ORDERED)
        .setFrame(d.getFrame().toBuilder().setClass_(DeliveryClass.DELIVERY_CLASS_ORDERED)).build()))
        .hasMessageContaining("ORDERED");
    PushRecord c = Fixtures.control("t1:u1", Fixtures.subscribe("t1:news"));
    assertThatThrownBy(() -> PushRecordRules.assertValid(c.toBuilder().setFrame(Fixtures.message("t1:u1").getFrame())
        .build())).hasMessageContaining("CONTROL");
  }
}
