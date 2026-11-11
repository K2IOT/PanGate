package com.pangate.contracts.v1;

import static java.util.Map.entry;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumDescriptor;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Pins the field and enum numbers of the push-path contract (push contracts spec §1, §3). */
class PushContractNumbersTest {

  static void assertNumbers(Descriptor d, Map<String, Integer> expected) {
    expected.forEach((name, number) -> {
      var f = d.findFieldByName(name);
      assertThat(f).as(d.getName() + "." + name).isNotNull();
      assertThat(f.getNumber()).as(d.getName() + "." + name).isEqualTo(number);
    });
    assertThat(d.getFields()).as(d.getName() + " has exactly the spec's fields").hasSize(expected.size());
  }

  static void assertValues(EnumDescriptor e, Map<String, Integer> expected) {
    expected.forEach((name, number) -> assertThat(e.findValueByName(name)).as(e.getName() + "." + name)
        .isNotNull().extracting(v -> v.getNumber()).isEqualTo(number));
    assertThat(e.getValues()).hasSize(expected.size());
  }

  @Test
  void pushRecordFieldNumbers() {
    assertNumbers(PushRecord.getDescriptor(), Map.ofEntries(entry("msg_id", 1), entry("kind", 2), entry("tenant_id", 3),
        entry("cell_id", 4), entry("principal_id", 5), entry("device", 6), entry("topic", 7), entry("class", 8),
        entry("priority", 9), entry("accepted_at_ms", 10), entry("expires_at_ms", 11), entry("collapse_key", 12),
        entry("receipt", 13), entry("sender_principal_id", 14), entry("frame", 15), entry("control", 16)));
    assertThat(PushRecord.getDescriptor().getRealOneofs()).extracting(o -> o.getName()).containsExactly("target");
    assertValues(PushRecord.Kind.getDescriptor(), Map.of("KIND_UNSPECIFIED", 0, "KIND_MESSAGE", 1, "KIND_BROADCAST", 2,
        "KIND_CONTROL", 3));
  }

  @Test
  void offlineAndDeliveredFieldNumbers() {
    assertNumbers(OfflineRecord.getDescriptor(), Map.of("record", 1, "reason", 2, "attempts", 3, "at_ms", 4));
    assertValues(OfflineRecord.Reason.getDescriptor(), Map.of("REASON_UNSPECIFIED", 0, "REASON_NO_SESSION", 1,
        "REASON_NACK_TIMEOUT", 2, "REASON_NODE_GONE", 3, "REASON_SLOW_CONSUMER", 4, "REASON_OVERFLOW", 5,
        "REASON_CLOSED", 6, "REASON_SHED", 7, "REASON_EXPIRED", 8));
    assertNumbers(DeliveredRecord.getDescriptor(), Map.of("msg_id", 1, "tenant_id", 2, "sender_principal_id", 3,
        "principal_id", 4, "device_id", 5, "conn_id", 6, "status", 7, "at_ms", 8));
  }

  @Test
  void pushRequestAndResponseExtensions() {
    assertNumbers(PushRequest.getDescriptor(), Map.ofEntries(entry("request_id", 1), entry("kind", 2),
        entry("tenant_id", 3), entry("principal_id", 4), entry("control", 5), entry("device_id", 6), entry("topic", 7),
        entry("class", 8), entry("priority", 9), entry("ttl_sec", 10), entry("collapse_key", 11), entry("receipt", 12),
        entry("idempotency_key", 13), entry("payload", 14), entry("headers", 15), entry("sender_principal_id", 16)));
    assertValues(PushRequest.Kind.getDescriptor(), Map.of("KIND_UNSPECIFIED", 0, "KIND_CONTROL", 1, "KIND_MESSAGE", 2,
        "KIND_BROADCAST", 3));
    assertNumbers(PushResponse.getDescriptor(), Map.ofEntries(entry("request_id", 1), entry("ok", 2), entry("error", 3),
        entry("code", 4), entry("msg_id", 5), entry("seq", 6), entry("seq_epoch", 7), entry("topic_seq", 8),
        entry("cell_id", 9), entry("duplicate", 10), entry("retry_after_ms", 11)));
    assertValues(PushResponse.Code.getDescriptor(), Map.of("CODE_UNSPECIFIED", 0, "CODE_OK", 1, "CODE_INVALID", 2,
        "CODE_FORBIDDEN", 3, "CODE_RATE_LIMITED", 4, "CODE_QUOTA_EXCEEDED", 5, "CODE_UNAVAILABLE", 6));
  }

  @Test
  void lifecycleAdditions() {
    assertThat(ConnectionEvent.getDescriptor().findFieldByName("event_id").getNumber()).isEqualTo(13);
    assertThat(EventType.EVENT_TYPE_NODE_DOWN.getNumber()).isEqualTo(5);
  }

  /** Push API rates (push-api spec §4.1), additive to the gateway's TenantLimits. */
  @Test
  void tenantLimitsPushFields() {
    assertNumbers(TenantLimits.getDescriptor(), Map.ofEntries(entry("max_conns", 1), entry("inbound_rate", 2),
        entry("inbound_burst", 3), entry("topics_per_conn", 4), entry("min_sdk_version", 5),
        entry("origin_allowlist", 6), entry("single_session", 7), entry("push_rate", 8), entry("push_burst", 9),
        entry("broadcast_rate", 10)));
  }
}
