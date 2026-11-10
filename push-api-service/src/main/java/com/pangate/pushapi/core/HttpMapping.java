package com.pangate.pushapi.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.pangate.contracts.v1.DeliveryClass;
import com.pangate.contracts.v1.PushItem;
import com.pangate.contracts.v1.PushRequest;
import com.pangate.contracts.v1.PushResponse;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/**
 * {@code POST /v1/push} JSON ↔ {@link PushRequest} / {@link PushResponse} (push contracts §3.2). The tenant always
 * comes from the API token; tenant fields in the body are ignored. Rules V1–V14 are checked afterwards, so a missing
 * field maps to an empty one and {@link PushRequests} names the rule.
 */
public final class HttpMapping {

  private static final ObjectMapper JSON = new ObjectMapper();

  private HttpMapping() {}

  private static BadRequestException bad(String detail) {
    return new BadRequestException("invalid_request", detail);
  }

  private static String string(Map<?, ?> m, String key, boolean required) {
    Object v = m.get(key);
    if (v == null) {
      if (required) {
        throw bad(key + " is required");
      }
      return "";
    }
    if (!(v instanceof String s)) {
      throw bad(key + " must be a string");
    }
    return s;
  }

  public static PushRequest toRequest(Map<String, Object> body, String tenant, String idempotencyKey) {
    if (!(body.get("target") instanceof Map<?, ?> target)) {
      throw bad("target is required");
    }
    PushRequest.Builder b = PushRequest.newBuilder().setTenantId(tenant);
    switch (string(target, "type", true)) {
      case "principal" -> b.setKind(PushRequest.Kind.KIND_MESSAGE).setPrincipalId(string(target, "id", true));
      case "device" -> b.setKind(PushRequest.Kind.KIND_MESSAGE).setPrincipalId(string(target, "principal", true))
          .setDeviceId(string(target, "id", true));
      case "topic" -> b.setKind(PushRequest.Kind.KIND_BROADCAST).setTopic(string(target, "id", true));
      default -> throw bad("target.type must be principal, device or topic");
    }
    String cls = string(body, "class", false);
    if (!cls.isEmpty()) {
      try {
        b.setClass_(DeliveryClass.valueOf("DELIVERY_CLASS_" + cls.toUpperCase(Locale.ROOT)));
      } catch (IllegalArgumentException e) {
        throw bad("class must be ephemeral, reliable or ordered");
      }
      if (b.getClass_() == DeliveryClass.DELIVERY_CLASS_UNSPECIFIED) {
        throw bad("class must be ephemeral, reliable or ordered");
      }
    }
    String prio = string(body, "priority", false);
    if (!prio.isEmpty()) {
      switch (prio.toLowerCase(Locale.ROOT)) {
        case "normal" -> b.setPriority(PushItem.Priority.PRIORITY_NORMAL);
        case "critical" -> b.setPriority(PushItem.Priority.PRIORITY_CRITICAL);
        default -> throw bad("priority must be normal or critical");
      }
    }
    Object ttl = body.get("ttlSec");
    if (ttl != null) {
      if (!(ttl instanceof Number n) || n.longValue() < 0 || n.longValue() > Integer.MAX_VALUE
          || n.doubleValue() != n.longValue()) {
        throw bad("ttlSec must be a non-negative integer");
      }
      b.setTtlSec((int) n.longValue());
    }
    b.setCollapseKey(string(body, "collapseKey", false));
    Object receipt = body.get("receipt");
    if (receipt != null) {
      if (!(receipt instanceof Boolean r)) {
        throw bad("receipt must be a boolean");
      }
      b.setReceipt(r);
    }
    b.setSenderPrincipalId(string(body, "senderPrincipalId", false));
    if (idempotencyKey != null) {
      b.setIdempotencyKey(idempotencyKey);
    }
    if (body.containsKey("payload") && body.get("payload") != null) {
      Object payload = body.get("payload");
      try {
        b.setPayload(ByteString.copyFrom(JSON.writeValueAsBytes(payload)));
      } catch (JsonProcessingException e) {
        throw bad("payload is not serializable");
      }
      b.putHeaders("content-type", "application/json");
      if (payload instanceof Map<?, ?> p && p.get("type") instanceof String type) {
        b.putHeaders("type", type);
      }
    }
    return b.build();
  }

  /** {@code 202} body: {@code {msgId, seq?, seqEpoch?, topicSeq?, cell, duplicate?}}, zero / false omitted. */
  public static Map<String, Object> toBody(PushResponse r) {
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("msgId", r.getMsgId());
    if (r.getSeq() != 0) {
      m.put("seq", r.getSeq());
    }
    if (r.getSeqEpoch() != 0) {
      m.put("seqEpoch", r.getSeqEpoch());
    }
    if (r.getTopicSeq() != 0) {
      m.put("topicSeq", r.getTopicSeq());
    }
    m.put("cell", r.getCellId());
    if (r.getDuplicate()) {
      m.put("duplicate", true);
    }
    return m;
  }
}
