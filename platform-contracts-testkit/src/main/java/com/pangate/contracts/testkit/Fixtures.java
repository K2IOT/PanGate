package com.pangate.contracts.testkit;

import com.google.protobuf.ByteString;
import com.pangate.contracts.v1.AckItem;
import com.pangate.contracts.v1.ConnectionEvent;
import com.pangate.contracts.v1.ControlOp;
import com.pangate.contracts.v1.DeliveredRecord;
import com.pangate.contracts.v1.DeliveryClass;
import com.pangate.contracts.v1.DeviceRef;
import com.pangate.contracts.v1.EventType;
import com.pangate.contracts.v1.Frame;
import com.pangate.contracts.v1.FrameType;
import com.pangate.contracts.v1.Kick;
import com.pangate.contracts.v1.OfflineRecord;
import com.pangate.contracts.v1.PushItem;
import com.pangate.contracts.v1.PushRecord;
import com.pangate.contracts.v1.PushRequest;
import com.pangate.contracts.v1.PushResponse;
import com.pangate.contracts.v1.Topics;
import java.nio.charset.StandardCharsets;

/**
 * Valid, deterministic records of the push path (push contracts spec §1.1). Change a field with {@code toBuilder()};
 * golden files are encoded from these, so never change a default without regenerating them deliberately.
 */
public final class Fixtures {

  public static final String FIXED_MSG_ID = "01JC0000000000000000000000";
  public static final String FIXED_EVENT_ID = "01JC00000000000000000EVENT";
  public static final long FIXED_NOW_MS = 1_762_732_800_000L;
  public static final long TTL_MS = 86_400_000L;
  public static final String CELL = "c1";
  public static final ByteString PAYLOAD = ByteString.copyFrom("{\"hello\":\"world\"}", StandardCharsets.UTF_8);

  private Fixtures() {}

  static String tenantOf(String principalOrTopic) {
    int i = principalOrTopic.indexOf(':');
    return i < 0 ? principalOrTopic : principalOrTopic.substring(0, i);
  }

  private static Frame.Builder frame(DeliveryClass c) {
    return Frame.newBuilder().setType(FrameType.FRAME_TYPE_PUSH).setMsgId(FIXED_MSG_ID).setClass_(c).setPayload(PAYLOAD)
        .putHeaders("type", "demo");
  }

  private static PushRecord.Builder base(PushRecord.Kind kind, String tenant, DeliveryClass c) {
    return PushRecord.newBuilder().setMsgId(FIXED_MSG_ID).setKind(kind).setTenantId(tenant).setCellId(CELL).setClass_(c)
        .setPriority(PushItem.Priority.PRIORITY_NORMAL).setAcceptedAtMs(FIXED_NOW_MS)
        .setExpiresAtMs(FIXED_NOW_MS + TTL_MS);
  }

  /** RELIABLE MESSAGE to every session of {@code principal}, seq 1 / epoch 1. */
  public static PushRecord message(String principal) {
    return base(PushRecord.Kind.KIND_MESSAGE, tenantOf(principal), DeliveryClass.DELIVERY_CLASS_RELIABLE)
        .setPrincipalId(principal).setFrame(frame(DeliveryClass.DELIVERY_CLASS_RELIABLE).setSeq(1).setSeqEpoch(1))
        .build();
  }

  /** RELIABLE MESSAGE to one device. */
  public static PushRecord deviceMessage(String principal, String device) {
    return base(PushRecord.Kind.KIND_MESSAGE, tenantOf(principal), DeliveryClass.DELIVERY_CLASS_RELIABLE)
        .setDevice(DeviceRef.newBuilder().setPrincipalId(principal).setDeviceId(device))
        .setFrame(frame(DeliveryClass.DELIVERY_CLASS_RELIABLE).setSeq(1).setSeqEpoch(1)).build();
  }

  /** EPHEMERAL MESSAGE: no seq. */
  public static PushRecord ephemeral(String principal) {
    return base(PushRecord.Kind.KIND_MESSAGE, tenantOf(principal), DeliveryClass.DELIVERY_CLASS_EPHEMERAL)
        .setPrincipalId(principal).setFrame(frame(DeliveryClass.DELIVERY_CLASS_EPHEMERAL)).build();
  }

  /** RELIABLE BROADCAST: frame.topic = topic, topic_seq 1. */
  public static PushRecord broadcast(String topic) {
    return base(PushRecord.Kind.KIND_BROADCAST, tenantOf(topic), DeliveryClass.DELIVERY_CLASS_RELIABLE).setTopic(topic)
        .setFrame(frame(DeliveryClass.DELIVERY_CLASS_RELIABLE).setTopic(topic).setTopicSeq(1)).build();
  }

  /** CONTROL for every session of {@code principal}; no frame. */
  public static PushRecord control(String principal, ControlOp op) {
    return PushRecord.newBuilder().setMsgId(FIXED_MSG_ID).setKind(PushRecord.Kind.KIND_CONTROL)
        .setTenantId(tenantOf(principal)).setCellId(CELL).setPrincipalId(principal)
        .setPriority(PushItem.Priority.PRIORITY_CRITICAL).setAcceptedAtMs(FIXED_NOW_MS)
        .setExpiresAtMs(FIXED_NOW_MS + TTL_MS).setControl(op).build();
  }

  public static ControlOp subscribe(String... topics) {
    return ControlOp.newBuilder().setSubscribe(Topics.newBuilder().addAllTopics(java.util.List.of(topics))).build();
  }

  public static ControlOp kick(String newConnId, long connectedAtMs) {
    return ControlOp.newBuilder().setKick(Kick.newBuilder().setNewConnId(newConnId).setConnectedAtMs(connectedAtMs))
        .build();
  }

  public static OfflineRecord offline(PushRecord r, OfflineRecord.Reason reason) {
    return OfflineRecord.newBuilder().setRecord(r).setReason(reason).setAttempts(3).setAtMs(FIXED_NOW_MS + 5_000)
        .build();
  }

  public static DeliveredRecord delivered(PushRecord r, String connId) {
    boolean device = r.getTargetCase() == PushRecord.TargetCase.DEVICE;
    return DeliveredRecord.newBuilder().setMsgId(r.getMsgId()).setTenantId(r.getTenantId())
        .setSenderPrincipalId(r.getSenderPrincipalId())
        .setPrincipalId(device ? r.getDevice().getPrincipalId() : r.getPrincipalId())
        .setDeviceId(device ? r.getDevice().getDeviceId() : "").setConnId(connId)
        .setStatus(AckItem.Status.STATUS_ACKED).setAtMs(FIXED_NOW_MS + 1_000).build();
  }

  public static ConnectionEvent nodeDown(String nodeId, long epoch) {
    return ConnectionEvent.newBuilder().setType(EventType.EVENT_TYPE_NODE_DOWN).setNodeId(nodeId).setEpoch(epoch)
        .setCellId(CELL).setAtMs(FIXED_NOW_MS).setEventId(FIXED_EVENT_ID).build();
  }

  /** Exactly the request gateway 0.1.0's PushApiKickClient sends: fields 1–5 only. */
  public static PushRequest kickRequest010() {
    return PushRequest.newBuilder().setRequestId("6f1c2b1e-0000-4000-8000-000000000001")
        .setKind(PushRequest.Kind.KIND_CONTROL).setTenantId("t1").setPrincipalId("t1:u1")
        .setControl(kick("01JCNEWCONN0000000000000000", FIXED_NOW_MS)).build();
  }

  /** A valid request of each kind. */
  public static PushRequest request(PushRequest.Kind kind) {
    PushRequest.Builder b = PushRequest.newBuilder().setRequestId("r-1").setKind(kind).setTenantId("t1");
    return switch (kind) {
      case KIND_MESSAGE -> b.setPrincipalId("t1:u1").setClass_(DeliveryClass.DELIVERY_CLASS_RELIABLE)
          .setTtlSec(3600).setPayload(PAYLOAD).putHeaders("type", "demo").build();
      case KIND_BROADCAST -> b.setTopic("t1:news").setClass_(DeliveryClass.DELIVERY_CLASS_RELIABLE).setPayload(PAYLOAD)
          .putHeaders("type", "demo").build();
      case KIND_CONTROL -> b.setPrincipalId("t1:u1").setControl(subscribe("t1:news")).build();
      default -> throw new IllegalArgumentException("no valid request of kind " + kind);
    };
  }

  public static PushResponse okResponse() {
    return PushResponse.newBuilder().setRequestId("r-1").setOk(true).setCode(PushResponse.Code.CODE_OK)
        .setMsgId(FIXED_MSG_ID).setSeq(1).setSeqEpoch(1).setCellId(CELL).build();
  }
}
