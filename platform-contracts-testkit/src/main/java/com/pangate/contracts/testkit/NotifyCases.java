package com.pangate.contracts.testkit;

import com.pangate.contracts.v1.Alert;
import com.pangate.contracts.v1.DeviceRef;
import com.pangate.contracts.v1.NotifyRequest;
import java.util.List;

/** Valid and invalid NotifyRequests for producers' and notify-service's tests (offline notifications spec §2.1). */
public final class NotifyCases {

  public record Case(String name, NotifyRequest request, String rule) {}

  private NotifyCases() {}

  static NotifyRequest base() {
    return NotifyRequest.newBuilder().setRequestId("01J0000000000000000000000M/chat:u2").setTenantId("chat")
        .setPrincipalId("chat:u2").setAlert(Alert.newBuilder().setTitle("Lan").setBody("mai họp lúc 9h"))
        .setBadge(3).setThreadId("01J0000000000000000000000C").setCollapseId("01J0000000000000000000000M")
        .putData("conversationId", "01J0000000000000000000000C").putData("convSeq", "7")
        .putData("msgId", "01J0000000000000000000000M").setCreatedAtMs(1_800_000_000_000L)
        .setExpiresAtMs(1_800_003_600_000L).setHighPriority(true).build();
  }

  public static List<Case> valid() {
    NotifyRequest b = base();
    return List.of(
        new Case("principal with title and body", b, ""),
        new Case("one device", b.toBuilder().setDevice(DeviceRef.newBuilder().setPrincipalId("chat:u2")
            .setDeviceId("d1")).build(), ""),
        new Case("localized, no preview", b.toBuilder().setAlert(Alert.newBuilder().setLocKey("notif_new_message"))
            .build(), ""),
        new Case("badge unchanged", b.toBuilder().setBadge(-1).build(), ""));
  }

  public static List<Case> invalid() {
    NotifyRequest b = base();
    return List.of(
        new Case("no request id", b.toBuilder().clearRequestId().build(), "request_id"),
        new Case("no tenant", b.toBuilder().clearTenantId().build(), "tenant_id"),
        new Case("no target", b.toBuilder().clearTarget().build(), "target"),
        new Case("empty principal", b.toBuilder().setPrincipalId("").build(), "target"),
        new Case("device without id", b.toBuilder().setDevice(DeviceRef.newBuilder().setPrincipalId("chat:u2"))
            .build(), "target"),
        new Case("expires before created", b.toBuilder().setExpiresAtMs(b.getCreatedAtMs()).build(), "expires"),
        new Case("empty alert", b.toBuilder().setAlert(Alert.getDefaultInstance()).build(), "alert"),
        new Case("alert too large", b.toBuilder().setAlert(Alert.newBuilder().setTitle("t")
            .setBody("x".repeat(NotifyRules.MAX_ALERT_BYTES))).build(), "alert_size"),
        new Case("badge below -1", b.toBuilder().setBadge(-2).build(), "badge"));
  }
}
