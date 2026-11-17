package com.pangate.notify.infrastructure.fcm;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pangate.contracts.v1.Alert;
import com.pangate.contracts.v1.NotifyRequest;
import org.junit.jupiter.api.Test;

/** Offline notifications spec §5.5: an FCM HTTP v1 message from a NotifyRequest. */
class FcmPayloadTest {

  static final ObjectMapper JSON = new ObjectMapper();
  static final long NOW = 1_800_000_000_000L;

  static NotifyRequest base() {
    return NotifyRequest.newBuilder().setRequestId("m1/chat:b").setTenantId("chat").setPrincipalId("chat:b")
        .setAlert(Alert.newBuilder().setTitle("Lan").setBody("hi")).setBadge(3).setThreadId("c1")
        .setCollapseId("m1").putData("conversationId", "c1").setCreatedAtMs(NOW).setExpiresAtMs(NOW + 3_600_000)
        .setHighPriority(true).build();
  }

  @Test
  void messageCarriesNotificationAndAndroidOptions() throws Exception {
    JsonNode m = JSON.readTree(FcmPayload.body("tok", base(), NOW)).get("message");
    assertThat(m.get("token").asText()).isEqualTo("tok");
    assertThat(m.at("/notification/title").asText()).isEqualTo("Lan");
    assertThat(m.at("/notification/body").asText()).isEqualTo("hi");
    assertThat(m.at("/android/notification/tag").asText()).isEqualTo("c1");
    assertThat(m.at("/android/collapse_key").asText()).isEqualTo("m1");
    assertThat(m.at("/android/priority").asText()).isEqualTo("HIGH");
    assertThat(m.at("/android/ttl").asText()).isEqualTo("3600s");
    assertThat(m.at("/android/notification/notification_count").asInt()).isEqualTo(3);
    assertThat(m.at("/data/conversationId").asText()).isEqualTo("c1");
  }

  @Test
  void locKeyNormalPriorityAndNoBadge() throws Exception {
    NotifyRequest r = base().toBuilder().setAlert(Alert.newBuilder().setLocKey("notif_new_message")).setBadge(-1)
        .setHighPriority(false).build();
    JsonNode m = JSON.readTree(FcmPayload.body("tok", r, NOW)).get("message");
    assertThat(m.at("/notification").isMissingNode()).isTrue();
    assertThat(m.at("/android/notification/body_loc_key").asText()).isEqualTo("notif_new_message");
    assertThat(m.at("/android/priority").asText()).isEqualTo("NORMAL");
    assertThat(m.at("/android/notification/notification_count").isMissingNode()).isTrue();
  }
}
