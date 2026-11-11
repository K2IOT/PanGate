package com.pangate.pushapi.core;

import com.pangate.contracts.v1.DeliveryClass;
import com.pangate.contracts.v1.DeviceRef;
import com.pangate.contracts.v1.Frame;
import com.pangate.contracts.v1.FrameType;
import com.pangate.contracts.v1.PushItem;
import com.pangate.contracts.v1.PushRecord;
import com.pangate.contracts.v1.PushRequest;
import java.time.Duration;
import java.util.function.LongSupplier;

/**
 * A valid {@link PushRequest} → {@link PushRecord} with a complete PUSH {@link Frame} (ADR 0017, push-api spec §2
 * step 8). Class UNSPECIFIED is RELIABLE, priority UNSPECIFIED is NORMAL, ttl 0 is the default ttl.
 */
public class RecordBuilder {

  private final String cell;
  private final LongSupplier wallMs;
  private final long defaultTtlMs;

  public RecordBuilder(String cell, LongSupplier wallMs, Duration defaultTtl) {
    this.cell = cell;
    this.wallMs = wallMs;
    this.defaultTtlMs = defaultTtl.toMillis();
  }

  static DeliveryClass effectiveClass(PushRequest r) {
    return r.getClass_() == DeliveryClass.DELIVERY_CLASS_UNSPECIFIED ? DeliveryClass.DELIVERY_CLASS_RELIABLE
        : r.getClass_();
  }

  /** {@code seq} is null when the record carries none (EPHEMERAL, device target, BROADCAST, CONTROL). */
  public PushRecord build(PushRequest r, String msgId, Seq seq, long topicSeq) {
    long now = wallMs.getAsLong();
    long ttlMs = r.getTtlSec() == 0 ? defaultTtlMs : r.getTtlSec() * 1000L;
    PushRecord.Builder b = PushRecord.newBuilder().setMsgId(msgId).setTenantId(r.getTenantId()).setCellId(cell)
        .setPriority(r.getPriority() == PushItem.Priority.PRIORITY_UNSPECIFIED ? PushItem.Priority.PRIORITY_NORMAL
            : r.getPriority())
        .setAcceptedAtMs(now).setExpiresAtMs(now + ttlMs).setCollapseKey(r.getCollapseKey())
        .setSenderPrincipalId(r.getSenderPrincipalId());
    switch (r.getKind()) {
      case KIND_CONTROL -> {
        return b.setKind(PushRecord.Kind.KIND_CONTROL).setPrincipalId(r.getPrincipalId()).setControl(r.getControl())
            .setPriority(PushItem.Priority.PRIORITY_CRITICAL).build();
      }
      case KIND_BROADCAST -> {
        DeliveryClass c = effectiveClass(r);
        Frame.Builder f = frame(r, msgId, c).setTopic(r.getTopic()).setTopicSeq(topicSeq);
        return b.setKind(PushRecord.Kind.KIND_BROADCAST).setTopic(r.getTopic()).setClass_(c).setFrame(f).build();
      }
      default -> {
        DeliveryClass c = effectiveClass(r);
        Frame.Builder f = frame(r, msgId, c);
        b.setKind(PushRecord.Kind.KIND_MESSAGE).setClass_(c).setReceipt(r.getReceipt());
        if (r.getDeviceId().isEmpty()) {
          b.setPrincipalId(r.getPrincipalId());
          if (seq != null) {
            f.setSeq(seq.seq()).setSeqEpoch(seq.epoch());
          }
        } else {
          b.setDevice(DeviceRef.newBuilder().setPrincipalId(r.getPrincipalId()).setDeviceId(r.getDeviceId()));
        }
        return b.setFrame(f).build();
      }
    }
  }

  private static Frame.Builder frame(PushRequest r, String msgId, DeliveryClass c) {
    return Frame.newBuilder().setType(FrameType.FRAME_TYPE_PUSH).setMsgId(msgId).setClass_(c)
        .setPayload(r.getPayload()).putAllHeaders(r.getHeadersMap());
  }
}
