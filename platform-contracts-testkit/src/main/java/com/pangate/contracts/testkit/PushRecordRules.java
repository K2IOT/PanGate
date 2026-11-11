package com.pangate.contracts.testkit;

import com.pangate.contracts.v1.ControlOp;
import com.pangate.contracts.v1.DeliveryClass;
import com.pangate.contracts.v1.Frame;
import com.pangate.contracts.v1.FrameType;
import com.pangate.contracts.v1.PushRecord;

/**
 * The rules of push contracts spec §1.1 as one check, for push-api (what it produces) and push-router tests (what they
 * feed in). Throws {@link AssertionError} naming the broken rule.
 */
public final class PushRecordRules {

  private PushRecordRules() {}

  private static void check(boolean ok, PushRecord r, String rule) {
    if (!ok) {
      throw new AssertionError("PushRecord " + r.getMsgId() + " breaks §1.1: " + rule);
    }
  }

  public static void assertValid(PushRecord r) {
    check(!r.getMsgId().isEmpty(), r, "msg_id is required");
    check(!r.getTenantId().isEmpty(), r, "tenant_id is required");
    check(r.getExpiresAtMs() >= r.getAcceptedAtMs(), r, "expires_at_ms >= accepted_at_ms");
    String prefix = r.getTenantId() + ":";
    switch (r.getKind()) {
      case KIND_MESSAGE -> message(r, prefix);
      case KIND_BROADCAST -> broadcast(r, prefix);
      case KIND_CONTROL -> {
        check(r.getTargetCase() == PushRecord.TargetCase.PRINCIPAL_ID, r, "CONTROL targets principal_id");
        check(r.getPrincipalId().startsWith(prefix), r, "principal_id inside the tenant");
        check(!r.hasFrame(), r, "CONTROL carries no frame");
        check(r.hasControl() && r.getControl().getOpCase() != ControlOp.OpCase.OP_NOT_SET, r, "CONTROL needs an op");
        check(!r.getReceipt(), r, "receipt only for MESSAGE");
      }
      default -> check(false, r, "kind is required");
    }
  }

  private static void frame(PushRecord r) {
    check(r.hasFrame(), r, r.getKind() + " needs a frame");
    Frame f = r.getFrame();
    check(f.getType() == FrameType.FRAME_TYPE_PUSH, r, "frame.type is PUSH");
    check(f.getMsgId().equals(r.getMsgId()), r, "frame.msg_id equals msg_id");
    check(f.getClass_() == r.getClass_(), r, "frame.class equals class");
    check(!r.hasControl(), r, r.getKind() + " carries no control");
  }

  private static boolean sequenced(PushRecord r) {
    return r.getClass_() == DeliveryClass.DELIVERY_CLASS_RELIABLE || r.getClass_() == DeliveryClass.DELIVERY_CLASS_ORDERED;
  }

  private static void message(PushRecord r, String prefix) {
    frame(r);
    Frame f = r.getFrame();
    check(f.getTopic().isEmpty() && f.getTopicSeq() == 0, r, "MESSAGE has no frame.topic / topic_seq");
    check(!(r.getReceipt() && r.getClass_() == DeliveryClass.DELIVERY_CLASS_EPHEMERAL), r,
        "receipt is not allowed with EPHEMERAL");
    switch (r.getTargetCase()) {
      case PRINCIPAL_ID -> {
        check(r.getPrincipalId().startsWith(prefix), r, "principal_id inside the tenant");
        check(sequenced(r) ? f.getSeq() > 0 : f.getSeq() == 0 && f.getSeqEpoch() == 0, r,
            "seq set exactly for RELIABLE / ORDERED");
      }
      case DEVICE -> {
        check(r.getDevice().getPrincipalId().startsWith(prefix), r, "device principal inside the tenant");
        check(!r.getDevice().getDeviceId().isEmpty(), r, "device target needs device_id");
        check(f.getSeq() == 0 && f.getSeqEpoch() == 0, r, "device target carries no seq");
        check(r.getClass_() != DeliveryClass.DELIVERY_CLASS_ORDERED, r, "device target cannot be ORDERED");
      }
      default -> check(false, r, "MESSAGE targets principal_id or device");
    }
  }

  private static void broadcast(PushRecord r, String prefix) {
    check(r.getTargetCase() == PushRecord.TargetCase.TOPIC, r, "BROADCAST targets topic");
    check(r.getTopic().startsWith(prefix), r, "topic inside the tenant");
    frame(r);
    Frame f = r.getFrame();
    check(f.getTopic().equals(r.getTopic()), r, "frame.topic equals topic");
    check(f.getSeq() == 0, r, "BROADCAST has no seq");
    check(sequenced(r) ? f.getTopicSeq() > 0 : f.getTopicSeq() == 0, r,
        "topic_seq set exactly for RELIABLE / ORDERED");
    check(!r.getReceipt(), r, "receipt only for MESSAGE");
  }
}
