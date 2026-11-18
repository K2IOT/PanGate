package com.pangate.contracts.testkit;

import com.pangate.contracts.v1.NotifyRequest;
import java.util.ArrayList;
import java.util.List;

/** What a NotifyRequest must hold (offline notifications spec §2.1). Producers check before sending, notify on read. */
public final class NotifyRules {

  /** Upper bound of the serialized Alert: APNs allows 4 KB for the whole payload. */
  public static final int MAX_ALERT_BYTES = 2_048;

  private NotifyRules() {}

  /** The rules the request breaks; empty when it is valid. */
  public static List<String> violations(NotifyRequest r) {
    List<String> out = new ArrayList<>();
    if (r.getRequestId().isEmpty()) {
      out.add("request_id");
    }
    if (r.getTenantId().isEmpty()) {
      out.add("tenant_id");
    }
    boolean target = switch (r.getTargetCase()) {
      case PRINCIPAL_ID -> !r.getPrincipalId().isEmpty();
      case DEVICE -> !r.getDevice().getPrincipalId().isEmpty() && !r.getDevice().getDeviceId().isEmpty();
      default -> false;
    };
    if (!target) {
      out.add("target");
    }
    if (r.getExpiresAtMs() <= r.getCreatedAtMs()) {
      out.add("expires");
    }
    boolean text = !r.getAlert().getTitle().isEmpty() || !r.getAlert().getBody().isEmpty();
    if (!text && r.getAlert().getLocKey().isEmpty()) {
      out.add("alert");
    }
    if (r.getAlert().getSerializedSize() > MAX_ALERT_BYTES) {
      out.add("alert_size");
    }
    if (r.getBadge() < -1) {
      out.add("badge");
    }
    return out;
  }
}
