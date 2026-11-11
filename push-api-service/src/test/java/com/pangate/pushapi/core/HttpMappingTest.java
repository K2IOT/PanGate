package com.pangate.pushapi.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.pangate.contracts.v1.DeliveryClass;
import com.pangate.contracts.v1.PushItem;
import com.pangate.contracts.v1.PushRequest;
import com.pangate.contracts.v1.PushResponse;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class HttpMappingTest {

  static Map<String, Object> body(Map<String, Object> target) {
    Map<String, Object> b = new LinkedHashMap<>();
    b.put("target", target);
    b.put("payload", Map.of("type", "chat.message", "text", "hi"));
    return b;
  }

  @Test
  void mappingFollowsContractsTable() {
    Map<String, Object> b = body(Map.of("type", "principal", "id", "t1:u1"));
    b.put("class", "ordered");
    b.put("ttlSec", 3600);
    b.put("priority", "critical");
    b.put("collapseKey", "c1");
    b.put("receipt", true);
    b.put("senderPrincipalId", "t1:svc");
    PushRequest r = HttpMapping.toRequest(b, "t1", "idem-1");
    assertThat(r.getKind()).isEqualTo(PushRequest.Kind.KIND_MESSAGE);
    assertThat(r.getTenantId()).isEqualTo("t1");
    assertThat(r.getPrincipalId()).isEqualTo("t1:u1");
    assertThat(r.getClass_()).isEqualTo(DeliveryClass.DELIVERY_CLASS_ORDERED);
    assertThat(r.getTtlSec()).isEqualTo(3600);
    assertThat(r.getPriority()).isEqualTo(PushItem.Priority.PRIORITY_CRITICAL);
    assertThat(r.getCollapseKey()).isEqualTo("c1");
    assertThat(r.getReceipt()).isTrue();
    assertThat(r.getSenderPrincipalId()).isEqualTo("t1:svc");
    assertThat(r.getIdempotencyKey()).isEqualTo("idem-1");
    assertThat(r.getHeadersMap()).containsEntry("type", "chat.message").containsEntry("content-type", "application/json");
    assertThat(r.getPayload().toString(StandardCharsets.UTF_8)).contains("\"text\":\"hi\"").contains("\"type\":\"chat.message\"");

    PushRequest d = HttpMapping.toRequest(body(Map.of("type", "device", "principal", "t1:u1", "id", "d1")), "t1", null);
    assertThat(d.getPrincipalId()).isEqualTo("t1:u1");
    assertThat(d.getDeviceId()).isEqualTo("d1");
    assertThat(d.getIdempotencyKey()).isEmpty();
    assertThat(d.getClass_()).isEqualTo(DeliveryClass.DELIVERY_CLASS_UNSPECIFIED);

    PushRequest t = HttpMapping.toRequest(body(Map.of("type", "topic", "id", "t1:news")), "t1", null);
    assertThat(t.getKind()).isEqualTo(PushRequest.Kind.KIND_BROADCAST);
    assertThat(t.getTopic()).isEqualTo("t1:news");
  }

  @Test
  void nonObjectPayloadHasNoTypeHeader() {
    Map<String, Object> b = new LinkedHashMap<>(Map.of("target", Map.of("type", "principal", "id", "t1:u1")));
    b.put("payload", List.of(1, 2));
    PushRequest r = HttpMapping.toRequest(b, "t1", null);
    assertThat(r.getHeadersMap()).doesNotContainKey("type").containsEntry("content-type", "application/json");
    assertThat(r.getPayload().toString(StandardCharsets.UTF_8)).isEqualTo("[1,2]");
  }

  @Test
  void missingPayloadMapsToEmptyPayloadForRuleV2() {
    PushRequest r = HttpMapping.toRequest(new LinkedHashMap<>(Map.of("target", Map.of("type", "principal", "id",
        "t1:u1"))), "t1", null);
    assertThat(r.getPayload().isEmpty()).isTrue();
    assertThat(PushRequests.violation(r)).get().extracting(Violation::rule).isEqualTo("V2");
  }

  /** Review Focus 2. */
  @Test
  void tenantComesFromTheTokenNeverTheBody() {
    Map<String, Object> b = body(Map.of("type", "principal", "id", "t1:u1"));
    b.put("tenant", "t2");
    b.put("tenantId", "t2");
    assertThat(HttpMapping.toRequest(b, "t1", null).getTenantId()).isEqualTo("t1");
  }

  @Test
  void controlIsNotMappableFromHttp() {
    assertThatThrownBy(() -> HttpMapping.toRequest(body(Map.of("type", "control", "id", "t1:u1")), "t1", null))
        .isInstanceOf(BadRequestException.class).extracting(e -> ((BadRequestException) e).code())
        .isEqualTo("invalid_request");
  }

  @Test
  void malformedShapesAreBadRequests() {
    assertThatThrownBy(() -> HttpMapping.toRequest(Map.of(), "t1", null)).isInstanceOf(BadRequestException.class);
    assertThatThrownBy(() -> HttpMapping.toRequest(body(Map.of("type", "principal", "id", 5)), "t1", null))
        .isInstanceOf(BadRequestException.class);
    Map<String, Object> b = body(Map.of("type", "principal", "id", "t1:u1"));
    b.put("class", "urgent");
    assertThatThrownBy(() -> HttpMapping.toRequest(b, "t1", null)).isInstanceOf(BadRequestException.class);
    Map<String, Object> neg = body(Map.of("type", "principal", "id", "t1:u1"));
    neg.put("ttlSec", -1);
    assertThatThrownBy(() -> HttpMapping.toRequest(neg, "t1", null)).isInstanceOf(BadRequestException.class);
  }

  @Test
  void responseBodyOmitsZeroFields() {
    Map<String, Object> m = HttpMapping.toBody(PushResponse.newBuilder().setOk(true).setCode(PushResponse.Code.CODE_OK)
        .setMsgId("M1").setCellId("c1").build());
    assertThat(m).containsOnlyKeys("msgId", "cell");
    Map<String, Object> full = HttpMapping.toBody(PushResponse.newBuilder().setMsgId("M2").setCellId("c1").setSeq(7)
        .setSeqEpoch(2).setTopicSeq(3).setDuplicate(true).build());
    assertThat(full).containsEntry("seq", 7L).containsEntry("seqEpoch", 2).containsEntry("topicSeq", 3L)
        .containsEntry("duplicate", true);
  }
}
