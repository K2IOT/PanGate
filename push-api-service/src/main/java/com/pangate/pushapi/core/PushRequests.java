package com.pangate.pushapi.core;

import com.pangate.contracts.v1.ControlOp;
import com.pangate.contracts.v1.DeliveryClass;
import com.pangate.contracts.v1.PushRequest;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Validity rules V1–V14 of push contracts §3.1, checked in order; the first broken rule is returned. */
public final class PushRequests {

  static final int MAX_ID_BYTES = 256;
  static final int MAX_DEVICE_BYTES = 128;
  static final int MAX_PAYLOAD_BYTES = 65_536;
  static final int MAX_TTL_SEC = 604_800;
  static final int MAX_HEADERS = 16;
  static final int MAX_HEADER_NAME_BYTES = 64;
  static final int MAX_HEADER_VALUE_BYTES = 1_024;
  static final int MAX_COLLAPSE_KEY_BYTES = 256;
  static final String WARMUP_TENANT = "__warmup";

  private PushRequests() {}

  private static final class Broken extends RuntimeException {
    final Violation v;

    Broken(String rule, String message) {
      super(message, null, false, false);
      v = new Violation(rule, message);
    }
  }

  private static void check(boolean ok, String rule, String message) {
    if (!ok) {
      throw new Broken(rule, message);
    }
  }

  static int bytes(String s) {
    return s.getBytes(StandardCharsets.UTF_8).length;
  }

  public static Optional<Violation> violation(PushRequest r) {
    try {
      validate(r);
      return Optional.empty();
    } catch (Broken b) {
      return Optional.of(b.v);
    }
  }

  private static void validate(PushRequest r) {
    PushRequest.Kind k = r.getKind();
    check(k == PushRequest.Kind.KIND_MESSAGE || k == PushRequest.Kind.KIND_BROADCAST
        || k == PushRequest.Kind.KIND_CONTROL, "V1", "kind is required");
    boolean hasControl = r.hasControl();
    switch (k) {
      case KIND_MESSAGE -> check(!r.getPrincipalId().isEmpty() && !r.getPayload().isEmpty() && r.getTopic().isEmpty()
          && !hasControl, "V2", "MESSAGE needs principal_id and payload, and no topic or control");
      case KIND_BROADCAST -> check(!r.getTopic().isEmpty() && !r.getPayload().isEmpty() && r.getPrincipalId().isEmpty()
          && r.getDeviceId().isEmpty() && !hasControl, "V3",
          "BROADCAST needs topic and payload, and no principal_id, device_id or control");
      default -> check(!r.getPrincipalId().isEmpty() && hasControl
          && r.getControl().getOpCase() != ControlOp.OpCase.OP_NOT_SET && r.getPayload().isEmpty()
          && r.getTopic().isEmpty(), "V4", "CONTROL needs principal_id and an op, and no payload or topic");
    }
    String prefix = r.getTenantId() + ":";
    if (!r.getPrincipalId().isEmpty()) {
      check(r.getPrincipalId().startsWith(prefix) && bytes(r.getPrincipalId()) <= MAX_ID_BYTES, "V5",
          "principal_id must start with '<tenant_id>:' and be at most 256 bytes");
    }
    if (!r.getTopic().isEmpty()) {
      check(r.getTopic().startsWith(prefix) && bytes(r.getTopic()) <= MAX_ID_BYTES, "V5",
          "topic must start with '<tenant_id>:' and be at most 256 bytes");
    }
    if (!r.getDeviceId().isEmpty()) {
      check(bytes(r.getDeviceId()) <= MAX_DEVICE_BYTES && r.getDeviceId().indexOf('/') < 0, "V6",
          "device_id is 1..128 bytes without '/'");
    }
    check(r.getPayload().size() <= MAX_PAYLOAD_BYTES && r.getTtlSec() <= MAX_TTL_SEC, "V7",
        "payload at most 65536 bytes, ttl_sec at most 604800");
    check(!r.getTenantId().isEmpty() && !WARMUP_TENANT.equals(r.getTenantId()), "V8", "tenant_id is required");
    DeliveryClass c = r.getClass_();
    boolean sequenced = c == DeliveryClass.DELIVERY_CLASS_UNSPECIFIED || c == DeliveryClass.DELIVERY_CLASS_RELIABLE
        || c == DeliveryClass.DELIVERY_CLASS_ORDERED;
    check(!r.getReceipt() || (k == PushRequest.Kind.KIND_MESSAGE && sequenced), "V10",
        "receipt only for RELIABLE / ORDERED MESSAGE");
    check(!(k == PushRequest.Kind.KIND_MESSAGE && !r.getDeviceId().isEmpty()
        && c == DeliveryClass.DELIVERY_CLASS_ORDERED), "V11", "a device MESSAGE cannot be ORDERED");
    check(r.getSenderPrincipalId().isEmpty() || r.getSenderPrincipalId().startsWith(prefix), "V12",
        "sender_principal_id must start with '<tenant_id>:'");
    if (k == PushRequest.Kind.KIND_CONTROL) {
      List<String> topics = switch (r.getControl().getOpCase()) {
        case SUBSCRIBE -> r.getControl().getSubscribe().getTopicsList();
        case UNSUBSCRIBE -> r.getControl().getUnsubscribe().getTopicsList();
        default -> List.of();
      };
      check(topics.stream().allMatch(t -> t.startsWith(prefix)) && r.getDeviceId().isEmpty(), "V13",
          "CONTROL topics must start with '<tenant_id>:'; no device_id");
    }
    Map<String, String> h = r.getHeadersMap();
    check(h.size() <= MAX_HEADERS && h.entrySet().stream().allMatch(e -> bytes(e.getKey()) <= MAX_HEADER_NAME_BYTES
        && bytes(e.getValue()) <= MAX_HEADER_VALUE_BYTES) && bytes(r.getCollapseKey()) <= MAX_COLLAPSE_KEY_BYTES,
        "V14", "at most 16 headers (name ≤ 64 B, value ≤ 1024 B); collapse_key ≤ 256 B");
  }
}
