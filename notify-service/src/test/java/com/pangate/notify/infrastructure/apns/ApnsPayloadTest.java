package com.pangate.notify.infrastructure.apns;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pangate.contracts.v1.Alert;
import com.pangate.contracts.v1.NotifyRequest;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Offline notifications spec §5.5: an APNs request from a NotifyRequest. */
class ApnsPayloadTest {

  static final ObjectMapper JSON = new ObjectMapper();

  static NotifyRequest base() {
    return NotifyRequest.newBuilder().setRequestId("m1/chat:b").setTenantId("chat").setPrincipalId("chat:b")
        .setAlert(Alert.newBuilder().setTitle("Lan").setBody("hi")).setBadge(3).setThreadId("c1")
        .setCollapseId("m1").putData("conversationId", "c1").setCreatedAtMs(1_800_000_000_000L)
        .setExpiresAtMs(1_800_003_600_000L).setHighPriority(true).build();
  }

  @Test
  void bodyCarriesAlertBadgeThreadAndData() throws Exception {
    JsonNode b = JSON.readTree(ApnsPayload.body(base()));
    assertThat(b.at("/aps/alert/title").asText()).isEqualTo("Lan");
    assertThat(b.at("/aps/alert/body").asText()).isEqualTo("hi");
    assertThat(b.at("/aps/badge").asInt()).isEqualTo(3);
    assertThat(b.at("/aps/thread-id").asText()).isEqualTo("c1");
    assertThat(b.at("/aps/sound").asText()).isEqualTo("default");
    assertThat(b.at("/conversationId").asText()).isEqualTo("c1");
  }

  @Test
  void locKeyAndUnchangedBadge() throws Exception {
    NotifyRequest r = base().toBuilder().setAlert(Alert.newBuilder().setLocKey("notif_new_message")
        .addLocArgs("x")).setBadge(-1).build();
    JsonNode b = JSON.readTree(ApnsPayload.body(r));
    assertThat(b.at("/aps/alert/loc-key").asText()).isEqualTo("notif_new_message");
    assertThat(b.at("/aps/alert/loc-args/0").asText()).isEqualTo("x");
    assertThat(b.at("/aps/alert/title").isMissingNode()).isTrue();
    assertThat(b.at("/aps/badge").isMissingNode()).isTrue();
  }

  @Test
  void headers() {
    Map<String, String> h = ApnsPayload.headers(base(), "com.pangate.chat");
    assertThat(h).containsEntry("apns-topic", "com.pangate.chat").containsEntry("apns-push-type", "alert")
        .containsEntry("apns-priority", "10").containsEntry("apns-collapse-id", "m1")
        .containsEntry("apns-expiration", "1800003600");
    assertThat(ApnsPayload.headers(base().toBuilder().setHighPriority(false).setCollapseId("").build(), "b"))
        .containsEntry("apns-priority", "5").doesNotContainKey("apns-collapse-id");
  }
}
