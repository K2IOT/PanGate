package com.pangate.pushapi.core;

import com.pangate.contracts.v1.ControlOp;
import com.pangate.contracts.v1.DeliveryClass;
import com.pangate.contracts.v1.Frame;
import com.pangate.contracts.v1.FrameType;
import com.pangate.contracts.v1.PushRecord;
import java.util.Optional;

/**
 * The rules of push contracts spec §1.1, as Push API applies them to every record it builds. Mirrors the testkit's
 * {@code PushRecordRules} (test scope only); {@code PushAcceptorTest} pins that both agree.
 */
public final class PushRecordCheck {

  private PushRecordCheck() {}

  private static final class Broken extends RuntimeException {
    Broken(String rule) {
      super(rule, null, false, false);
    }
  }

  private static void check(boolean ok, String rule) {
    if (!ok) {
      throw new Broken(rule);
    }
  }

  /** The first rule {@code r} breaks, or empty if it is valid. */
  public static Optional<String> violation(PushRecord r) {
    try {
      validate(r);
      return Optional.empty();
    } catch (Broken b) {
      return Optional.of(b.getMessage());
    }
  }

  private static void validate(PushRecord r) {
    check(!r.getMsgId().isEmpty(), "msg_id is required");
    check(!r.getTenantId().isEmpty(), "tenant_id is required");
    check(r.getExpiresAtMs() >= r.getAcceptedAtMs(), "expires_at_ms >= accepted_at_ms");
    String prefix = r.getTenantId() + ":";
    switch (r.getKind()) {
      case KIND_MESSAGE -> message(r, prefix);
      case KIND_BROADCAST -> broadcast(r, prefix);
      case KIND_CONTROL -> {
        check(r.getTargetCase() == PushRecord.TargetCase.PRINCIPAL_ID, "CONTROL targets principal_id");
        check(r.getPrincipalId().startsWith(prefix), "principal_id inside the tenant");
        check(!r.hasFrame(), "CONTROL carries no frame");
        check(r.hasControl() && r.getControl().getOpCase() != ControlOp.OpCase.OP_NOT_SET, "CONTROL needs an op");
        check(!r.getReceipt(), "receipt only for MESSAGE");
      }
      default -> check(false, "kind is required");
    }
  }

  private static boolean sequenced(PushRecord r) {
    return r.getClass_() == DeliveryClass.DELIVERY_CLASS_RELIABLE || r.getClass_() == DeliveryClass.DELIVERY_CLASS_ORDERED;
  }

  private static void frame(PushRecord r) {
    check(r.hasFrame(), "a frame is required");
    Frame f = r.getFrame();
    check(f.getType() == FrameType.FRAME_TYPE_PUSH, "frame.type is PUSH");
    check(f.getMsgId().equals(r.getMsgId()), "frame.msg_id equals msg_id");
    check(f.getClass_() == r.getClass_(), "frame.class equals class");
    check(!r.hasControl(), "no control outside CONTROL");
  }

  private static void message(PushRecord r, String prefix) {
    frame(r);
    Frame f = r.getFrame();
    check(f.getTopic().isEmpty() && f.getTopicSeq() == 0, "MESSAGE has no frame.topic / topic_seq");
    check(!(r.getReceipt() && r.getClass_() == DeliveryClass.DELIVERY_CLASS_EPHEMERAL), "no receipt with EPHEMERAL");
    switch (r.getTargetCase()) {
      case PRINCIPAL_ID -> {
        check(r.getPrincipalId().startsWith(prefix), "principal_id inside the tenant");
        check(sequenced(r) ? f.getSeq() > 0 : f.getSeq() == 0 && f.getSeqEpoch() == 0,
            "seq set exactly for RELIABLE / ORDERED");
      }
      case DEVICE -> {
        check(r.getDevice().getPrincipalId().startsWith(prefix), "device principal inside the tenant");
        check(!r.getDevice().getDeviceId().isEmpty(), "device target needs device_id");
        check(f.getSeq() == 0 && f.getSeqEpoch() == 0, "device target carries no seq");
        check(r.getClass_() != DeliveryClass.DELIVERY_CLASS_ORDERED, "device target cannot be ORDERED");
      }
      default -> check(false, "MESSAGE targets principal_id or device");
    }
  }

  private static void broadcast(PushRecord r, String prefix) {
    check(r.getTargetCase() == PushRecord.TargetCase.TOPIC, "BROADCAST targets topic");
    check(r.getTopic().startsWith(prefix), "topic inside the tenant");
    frame(r);
    Frame f = r.getFrame();
    check(f.getTopic().equals(r.getTopic()), "frame.topic equals topic");
    check(f.getSeq() == 0, "BROADCAST has no seq");
    check(sequenced(r) ? f.getTopicSeq() > 0 : f.getTopicSeq() == 0, "topic_seq set exactly for RELIABLE / ORDERED");
    check(!r.getReceipt(), "receipt only for MESSAGE");
  }
}
