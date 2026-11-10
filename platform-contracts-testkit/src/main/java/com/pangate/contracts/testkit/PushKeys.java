package com.pangate.contracts.testkit;

import com.pangate.contracts.v1.ConnectionEvent;
import com.pangate.contracts.v1.DeliveredRecord;
import com.pangate.contracts.v1.EventType;
import com.pangate.contracts.v1.OfflineRecord;
import com.pangate.contracts.v1.PushRecord;

/** Record keys of the push path (push contracts spec §2): per-principal order, broadcasts spread by topic. */
public final class PushKeys {

  public static final String TOPIC_PREFIX = "topic:";

  private PushKeys() {}

  public static String of(PushRecord r) {
    return switch (r.getTargetCase()) {
      case PRINCIPAL_ID -> r.getPrincipalId();
      case DEVICE -> r.getDevice().getPrincipalId();
      case TOPIC -> TOPIC_PREFIX + r.getTopic();
      case TARGET_NOT_SET -> throw new IllegalArgumentException("PushRecord " + r.getMsgId() + " has no target");
    };
  }

  public static String of(OfflineRecord r) {
    return of(r.getRecord());
  }

  public static String of(DeliveredRecord r) {
    return r.getSenderPrincipalId().isEmpty() ? r.getMsgId() : r.getSenderPrincipalId();
  }

  public static String of(ConnectionEvent e) {
    return e.getType() == EventType.EVENT_TYPE_NODE_DOWN ? e.getNodeId() : e.getPrincipalId();
  }
}
