package com.pangate.chat.core;

import com.google.protobuf.ByteString;
import com.pangate.contracts.testkit.ChatTopics;
import com.pangate.contracts.v1.DeliveryClass;
import com.pangate.contracts.v1.PushRequest;

/** CH3 spec §1: every conversation-state push is an EPHEMERAL MESSAGE (state that is stale is worthless). */
public final class StatePushes {

  public static final int RECEIPTS_TTL_SEC = 30;
  public static final int TYPING_TTL_SEC = 10;
  public static final int PRESENCE_TTL_SEC = 30;

  private StatePushes() {}

  /** {@code deviceIdOrEmpty}: one device of the user, or every device when empty. */
  public static PushRequest ephemeral(String userId, String deviceIdOrEmpty, String type, ByteString payload,
      int ttlSec) {
    PushRequest.Builder b = PushRequest.newBuilder().setRequestId(Ids.next()).setKind(PushRequest.Kind.KIND_MESSAGE)
        .setTenantId(ChatTopics.TENANT).setPrincipalId(UserIds.principal(userId))
        .setClass_(DeliveryClass.DELIVERY_CLASS_EPHEMERAL).putHeaders("type", type).setPayload(payload)
        .setTtlSec(ttlSec);
    if (deviceIdOrEmpty != null && !deviceIdOrEmpty.isEmpty()) {
      b.setDeviceId(deviceIdOrEmpty);
    }
    return b.build();
  }
}
