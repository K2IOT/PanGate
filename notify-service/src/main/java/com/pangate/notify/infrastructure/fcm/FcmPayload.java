package com.pangate.notify.infrastructure.fcm;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pangate.contracts.v1.Alert;
import com.pangate.contracts.v1.NotifyRequest;
import java.util.LinkedHashMap;
import java.util.Map;

/** An FCM HTTP v1 message from a NotifyRequest (offline notifications spec §5.5). */
public final class FcmPayload {

  private static final ObjectMapper JSON = new ObjectMapper();

  private FcmPayload() {}

  public static String body(String token, NotifyRequest r, long nowMs) {
    Alert a = r.getAlert();
    Map<String, Object> androidNote = new LinkedHashMap<>();
    Map<String, Object> message = new LinkedHashMap<>();
    message.put("token", token);
    if (a.getLocKey().isEmpty()) {
      Map<String, Object> n = new LinkedHashMap<>();
      if (!a.getTitle().isEmpty()) {
        n.put("title", a.getTitle());
      }
      n.put("body", a.getBody());
      message.put("notification", n);
    } else {
      androidNote.put("body_loc_key", a.getLocKey());
      if (a.getLocArgsCount() > 0) {
        androidNote.put("body_loc_args", a.getLocArgsList());
      }
    }
    if (!r.getThreadId().isEmpty()) {
      androidNote.put("tag", r.getThreadId());
    }
    if (r.getBadge() >= 0) {
      androidNote.put("notification_count", r.getBadge());
    }
    Map<String, Object> android = new LinkedHashMap<>();
    if (!r.getCollapseId().isEmpty()) {
      android.put("collapse_key", r.getCollapseId());
    }
    android.put("priority", r.getHighPriority() ? "HIGH" : "NORMAL");
    android.put("ttl", Math.max(0, (r.getExpiresAtMs() - nowMs) / 1000) + "s");
    if (!androidNote.isEmpty()) {
      android.put("notification", androidNote);
    }
    message.put("android", android);
    if (r.getDataCount() > 0) {
      message.put("data", r.getDataMap());
    }
    try {
      return JSON.writeValueAsString(Map.of("message", message));
    } catch (JsonProcessingException e) {
      throw new IllegalStateException(e);
    }
  }
}
