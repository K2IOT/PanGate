package com.pangate.pushapi.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.contracts.testkit.Fixtures;
import com.pangate.contracts.testkit.PushRecordRules;
import com.pangate.contracts.v1.DeliveryClass;
import com.pangate.contracts.v1.FrameType;
import com.pangate.contracts.v1.PushItem;
import com.pangate.contracts.v1.PushRecord;
import com.pangate.contracts.v1.PushRequest;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class RecordBuilderTest {

  static final long NOW = 1_800_000_000_000L;
  final RecordBuilder b = new RecordBuilder("c1", () -> NOW, Duration.ofSeconds(86_400));

  static PushRequest msg(DeliveryClass c) {
    return Fixtures.request(PushRequest.Kind.KIND_MESSAGE).toBuilder().setClass_(c).build();
  }

  @Test
  void everyBuiltRecordPassesPushRecordRules() {
    String id = Ulids.next(NOW);
    for (DeliveryClass c : new DeliveryClass[] {DeliveryClass.DELIVERY_CLASS_RELIABLE,
        DeliveryClass.DELIVERY_CLASS_ORDERED, DeliveryClass.DELIVERY_CLASS_UNSPECIFIED}) {
      PushRecordRules.assertValid(b.build(msg(c), id, new Seq(5, 1), 0));
    }
    PushRecordRules.assertValid(b.build(msg(DeliveryClass.DELIVERY_CLASS_EPHEMERAL), id, null, 0));
    PushRecordRules.assertValid(b.build(msg(DeliveryClass.DELIVERY_CLASS_RELIABLE).toBuilder().setDeviceId("d1")
        .build(), id, null, 0));
    PushRequest bc = Fixtures.request(PushRequest.Kind.KIND_BROADCAST);
    PushRecordRules.assertValid(b.build(bc, id, null, 9));
    PushRecordRules.assertValid(b.build(bc.toBuilder().setClass_(DeliveryClass.DELIVERY_CLASS_EPHEMERAL).build(),
        id, null, 0));
    PushRecordRules.assertValid(b.build(Fixtures.kickRequest010(), id, null, 0));
    PushRecordRules.assertValid(b.build(Fixtures.request(PushRequest.Kind.KIND_CONTROL), id, null, 0));
  }

  @Test
  void frameFollowsAdr0017() {
    PushRequest r = msg(DeliveryClass.DELIVERY_CLASS_UNSPECIFIED).toBuilder().setCollapseKey("ck").setReceipt(true)
        .setSenderPrincipalId("t1:svc").build();
    PushRecord rec = b.build(r, "MID", new Seq(5, 2), 0);
    assertThat(rec.getMsgId()).isEqualTo("MID");
    assertThat(rec.getCellId()).isEqualTo("c1");
    assertThat(rec.getClass_()).isEqualTo(DeliveryClass.DELIVERY_CLASS_RELIABLE);
    assertThat(rec.getReceipt()).isTrue();
    assertThat(rec.getSenderPrincipalId()).isEqualTo("t1:svc");
    assertThat(rec.getCollapseKey()).isEqualTo("ck");
    var f = rec.getFrame();
    assertThat(f.getType()).isEqualTo(FrameType.FRAME_TYPE_PUSH);
    assertThat(f.getMsgId()).isEqualTo("MID");
    assertThat(f.getClass_()).isEqualTo(DeliveryClass.DELIVERY_CLASS_RELIABLE);
    assertThat(f.getPayload()).isEqualTo(r.getPayload());
    assertThat(f.getHeadersMap()).isEqualTo(r.getHeadersMap());
    assertThat(f.getSeq()).isEqualTo(5);
    assertThat(f.getSeqEpoch()).isEqualTo(2);
    PushRecord bc = b.build(Fixtures.request(PushRequest.Kind.KIND_BROADCAST), "MB", null, 9);
    assertThat(bc.getFrame().getTopic()).isEqualTo("t1:news");
    assertThat(bc.getFrame().getTopicSeq()).isEqualTo(9);
  }

  @Test
  void ttlDefaultsAndExpiry() {
    assertThat(b.build(msg(DeliveryClass.DELIVERY_CLASS_RELIABLE).toBuilder().setTtlSec(0).build(), "M", new Seq(1, 0),
        0).getExpiresAtMs()).isEqualTo(NOW + 86_400_000L);
    PushRecord r = b.build(msg(DeliveryClass.DELIVERY_CLASS_RELIABLE).toBuilder().setTtlSec(3600).build(), "M",
        new Seq(1, 0), 0);
    assertThat(r.getAcceptedAtMs()).isEqualTo(NOW);
    assertThat(r.getExpiresAtMs()).isEqualTo(NOW + 3_600_000L);
  }

  @Test
  void priorityUnspecifiedIsNormal() {
    assertThat(b.build(msg(DeliveryClass.DELIVERY_CLASS_RELIABLE), "M", new Seq(1, 0), 0).getPriority())
        .isEqualTo(PushItem.Priority.PRIORITY_NORMAL);
  }

  @Test
  void ulidsAreSortableAnd26Chars() {
    String a = Ulids.next(NOW);
    String c = Ulids.next(NOW + 1);
    assertThat(a).hasSize(26).matches("[0-9A-HJKMNP-TV-Z]{26}");
    assertThat(a.compareTo(c)).isNegative();
  }
}
