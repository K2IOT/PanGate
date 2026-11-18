package com.pangate.notify.infrastructure.apns;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pangate.contracts.v1.Alert;
import com.pangate.contracts.v1.NotifyRequest;
import java.util.LinkedHashMap;
import java.util.Map;

/** An APNs request from a NotifyRequest (offline notifications spec §5.5). */
public final class ApnsPayload {

  private static final ObjectMapper JSON = new ObjectMapper();

  private ApnsPayload() {}

  public static String body(NotifyRequest r) {
    Map<String, Object> alert = new LinkedHashMap<>();
    Alert a = r.getAlert();
    if (!a.getLocKey().isEmpty()) {
      alert.put("loc-key", a.getLocKey());
      if (a.getLocArgsCount() > 0) {
        alert.put("loc-args", a.getLocArgsList());
      }
    } else {
      if (!a.getTitle().isEmpty()) {
        alert.put("title", a.getTitle());
      }
      alert.put("body", a.getBody());
    }
    Map<String, Object> aps = new LinkedHashMap<>();
    aps.put("alert", alert);
    if (r.getBadge() >= 0) {
      aps.put("badge", r.getBadge());
    }
    if (!r.getThreadId().isEmpty()) {
      aps.put("thread-id", r.getThreadId());
    }
    aps.put("sound", "default");
    Map<String, Object> root = new LinkedHashMap<>(r.getDataMap());
    root.put("aps", aps);
    try {
      return JSON.writeValueAsString(root);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException(e);
    }
  }

  public static Map<String, String> headers(NotifyRequest r, String bundleId) {
    Map<String, String> h = new LinkedHashMap<>();
    h.put("apns-topic", bundleId);
    h.put("apns-push-type", "alert");
    h.put("apns-priority", r.getHighPriority() ? "10" : "5");
    h.put("apns-expiration", Long.toString(r.getExpiresAtMs() / 1000));
    if (!r.getCollapseId().isEmpty()) {
      h.put("apns-collapse-id", r.getCollapseId());
    }
    return h;
  }
}
