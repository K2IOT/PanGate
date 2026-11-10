package com.pangate.pushapi.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pangate.contracts.testkit.PushPathScenario;
import com.pangate.contracts.testkit.PushTopics;
import com.pangate.contracts.v1.AckItem;
import com.pangate.contracts.v1.ControlEvent;
import com.pangate.contracts.v1.DeliveredRecord;
import com.pangate.contracts.v1.KillSwitches;
import com.pangate.contracts.v1.OfflineRecord;
import com.pangate.contracts.v1.PushRecord;
import com.pangate.contracts.v1.Revoke;
import com.pangate.contracts.v1.Scope;
import com.pangate.contracts.v1.TenantCfg;
import com.pangate.contracts.v1.TenantLimits;
import com.pangate.pushapi.core.ControlState;
import com.pangate.pushapi.support.PushApiHarness;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

class PushHttpIT {

  @RegisterExtension static PushPathScenario scenario = new PushPathScenario("c1");
  static final GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7.4.2"))
      .withExposedPorts(6379);
  static PushApiHarness api;
  static KafkaProducer<String, byte[]> producer;
  static final ObjectMapper JSON = new ObjectMapper();
  static final String SEND = "push:send push:broadcast";

  @BeforeAll
  static void boot() throws Exception {
    redis.start();
    try (Admin admin = Admin.create(Map.of("bootstrap.servers", scenario.bootstrapServers()))) {
      try {
        admin.createTopics(List.of(new NewTopic("gw.control", 8, (short) 1)
            .configs(Map.of("cleanup.policy", "compact")))).all().get();
      } catch (Exception e) {
        // exists
      }
    }
    producer = new KafkaProducer<>(Map.of("bootstrap.servers", scenario.bootstrapServers()), new StringSerializer(),
        new ByteArraySerializer());
    api = new PushApiHarness(scenario.bootstrapServers(),
        "redis://" + redis.getHost() + ":" + redis.getMappedPort(6379));
  }

  @AfterAll
  static void shutdown() {
    if (api != null) {
      api.close();
    }
    producer.close();
    redis.stop();
  }

  static String push(String principal) {
    return "{\"target\":{\"type\":\"principal\",\"id\":\"" + principal + "\"},\"payload\":{\"type\":\"t\",\"n\":1}}";
  }

  static JsonNode json(HttpResponse<String> r) throws Exception {
    return JSON.readTree(r.body());
  }

  static void control(String key, ControlEvent e) throws Exception {
    producer.send(new ProducerRecord<>("gw.control", key, e == null ? null : e.toByteArray())).get();
  }

  static void cfg(String tenant, TenantLimits l, KillSwitches k) throws Exception {
    TenantCfg.Builder c = TenantCfg.newBuilder().setScope(Scope.SCOPE_TENANT).setScopeId(tenant);
    if (l != null) {
      c.setLimits(l);
    }
    if (k != null) {
      c.setSwitches(k);
    }
    control("tenantcfg:tenant:" + tenant, ControlEvent.newBuilder().setTenantCfg(c).build());
  }

  static String u() {
    return "t1:u-" + UUID.randomUUID().toString().substring(0, 8);
  }

  // --- tests ---------------------------------------------------------------------------------------------------

  @Test
  void pushReachesKafkaWith202() throws Exception {
    String p = u();
    HttpResponse<String> r = api.post(api.http, "/v1/push", api.token("t1", "t1-backend", SEND), push(p));
    assertThat(r.statusCode()).as(r.body()).isEqualTo(202);
    JsonNode b = json(r);
    assertThat(b.get("seq").asLong()).isEqualTo(1);
    assertThat(b.get("cell").asText()).isEqualTo("c1");
    PushRecord rec = scenario.awaitRequest(b.get("msgId").asText());
    assertThat(rec.getPrincipalId()).isEqualTo(p);
    assertThat(rec.getFrame().getHeadersMap()).containsEntry("type", "t");
    assertThat(api.ctx.getBean(io.micrometer.core.instrument.MeterRegistry.class).find("pushapi.accept.latency")
        .tag("kind", "message").timer()).isNotNull().satisfies(t -> assertThat(t.count()).isPositive());
  }

  /** Review Focus 2. */
  @Test
  void bodyTenantIsIgnored() throws Exception {
    String p = u();
    String body = push(p).replace("{\"target\"", "{\"tenant\":\"t2\",\"tenantId\":\"t2\",\"target\"");
    HttpResponse<String> r = api.post(api.http, "/v1/push", api.token("t1", "t1-backend", SEND), body);
    assertThat(r.statusCode()).isEqualTo(202);
    assertThat(scenario.awaitRequest(json(r).get("msgId").asText()).getTenantId()).isEqualTo("t1");
  }

  @Test
  void invalidIs400ProblemWithRule() throws Exception {
    HttpResponse<String> r = api.post(api.http, "/v1/push", api.token("t1", "t1-backend", SEND),
        "{\"target\":{\"type\":\"principal\",\"id\":\"t1:u1\"}}");
    assertThat(r.statusCode()).isEqualTo(400);
    assertThat(r.headers().firstValue("Content-Type")).get().asString().startsWith("application/problem+json");
    assertThat(json(r).get("code").asText()).isEqualTo("rule_V2");
    HttpResponse<String> notJson = api.post(api.http, "/v1/push", api.token("t1", "t1-backend", SEND), "[1,2]");
    assertThat(notJson.statusCode()).isEqualTo(400);
    assertThat(json(notJson).get("code").asText()).isEqualTo("invalid_request");
  }

  @Test
  void missingScopeIs403AndNoTokenIs401() throws Exception {
    HttpResponse<String> none = api.post(api.http, "/v1/push", null, push(u()));
    assertThat(none.statusCode()).isEqualTo(401);
    assertThat(none.headers().firstValue("WWW-Authenticate")).isPresent();
    HttpResponse<String> scope = api.post(api.http, "/v1/push", api.token("t1", "t1-backend", "tokens:mint"),
        push(u()));
    assertThat(scope.statusCode()).isEqualTo(403);
    assertThat(json(scope).get("code").asText()).isEqualTo("forbidden");
  }

  @Test
  void parallelSameIdempotencyKeyProducesOnce() throws Exception {
    String p = u();
    String token = api.token("t1", "t1-backend", SEND);
    String key = UUID.randomUUID().toString();
    List<CompletableFuture<HttpResponse<String>>> calls = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      calls.add(CompletableFuture.supplyAsync(() -> {
        try {
          return api.post(api.http, "/v1/push", token, push(p), "Idempotency-Key", key);
        } catch (Exception e) {
          throw new IllegalStateException(e);
        }
      }));
    }
    List<JsonNode> bodies = new ArrayList<>();
    for (var c : calls) {
      HttpResponse<String> r = c.get();
      assertThat(r.statusCode()).as(r.body()).isIn(202, 503);
      if (r.statusCode() == 202) {
        bodies.add(json(r));
      }
    }
    HttpResponse<String> again = api.post(api.http, "/v1/push", token, push(p), "Idempotency-Key", key);
    assertThat(json(again).get("duplicate").asBoolean()).isTrue();
    String msgId = json(again).get("msgId").asText();
    bodies.forEach(b -> assertThat(b.get("msgId").asText()).isEqualTo(msgId));
    scenario.awaitRequest(msgId);
    long copies = scenario.drain(PushTopics.requests("c1").name()).stream().filter(r -> {
      try {
        return PushRecord.parseFrom(r.value()).getMsgId().equals(msgId);
      } catch (Exception e) {
        return false;
      }
    }).count();
    assertThat(copies).isEqualTo(1);
  }

  /** Review Focus 4. */
  @Test
  void batchReturnsPerIndexAndKeepsPrincipalOrder() throws Exception {
    String u1 = u();
    String u2 = u();
    String body = "{\"items\":[" + push(u1) + ",{\"target\":{\"type\":\"principal\",\"id\":\"" + u1 + "\"}},"
        + push(u1) + "," + push(u2) + "]}";
    HttpResponse<String> r = api.post(api.http, "/v1/push:batch", api.token("t1", "t1-backend", SEND), body);
    assertThat(r.statusCode()).as(r.body()).isEqualTo(200);
    JsonNode results = json(r).get("results");
    assertThat(results).hasSize(4);
    List<Integer> statuses = new ArrayList<>();
    results.forEach(x -> statuses.add(x.get("status").asInt()));
    assertThat(statuses).containsExactly(202, 400, 202, 202);
    assertThat(results.get(1).get("code").asText()).isEqualTo("rule_V2");
    String first = results.get(0).get("msgId").asText();
    String third = results.get(2).get("msgId").asText();
    assertThat(results.get(0).get("seq").asLong()).isEqualTo(1);
    assertThat(results.get(2).get("seq").asLong()).isEqualTo(2);
    scenario.awaitRequest(third);
    List<String> u1Order = new ArrayList<>();
    for (var rec : scenario.drain(PushTopics.requests("c1").name())) {
      PushRecord pr = PushRecord.parseFrom(rec.value());
      if (pr.getPrincipalId().equals(u1)) {
        u1Order.add(pr.getMsgId());
      }
    }
    assertThat(u1Order).containsExactly(first, third);
  }

  @Test
  void batchOver100Is400AndOver1MbIs413() throws Exception {
    String token = api.token("t1", "t1-backend", SEND);
    StringBuilder b = new StringBuilder("{\"items\":[");
    for (int i = 0; i < 101; i++) {
      b.append(i == 0 ? "" : ",").append(push("t1:u1"));
    }
    HttpResponse<String> many = api.post(api.http, "/v1/push:batch", token, b.append("]}").toString());
    assertThat(many.statusCode()).isEqualTo(400);
    assertThat(json(many).get("code").asText()).isEqualTo("batch_too_large");
    String big = "{\"items\":[{\"target\":{\"type\":\"principal\",\"id\":\"t1:u1\"},\"payload\":\"" + "x".repeat(1_100_000)
        + "\"}]}";
    assertThat(api.post(api.http, "/v1/push:batch", token, big).statusCode()).isEqualTo(413);
    String single = "{\"target\":{\"type\":\"principal\",\"id\":\"t1:u1\"},\"payload\":\"" + "x".repeat(140_000) + "\"}";
    assertThat(api.post(api.http, "/v1/push", token, single).statusCode()).isEqualTo(413);
  }

  @Test
  void revokedClientIs403WithinOneSecond() throws Exception {
    String token = api.token("t1", "t1-revoked", SEND);
    assertThat(api.post(api.http, "/v1/push", token, push(u())).statusCode()).isEqualTo(202);
    control("revoke:c:t1-revoked", ControlEvent.newBuilder().setRevoke(Revoke.newBuilder().setClientId("t1-revoked")
        .setExpiresAtMs(System.currentTimeMillis() + 60_000)).setIssuedAtMs(System.currentTimeMillis()).build());
    await().atMost(Duration.ofSeconds(1)).pollInterval(Duration.ofMillis(50))
        .until(() -> api.post(api.http, "/v1/push", token, push(u())).statusCode() == 403);
    // a token issued after the event works (Review Focus 3)
    String fresh = api.token("t1", "t1-revoked", SEND, UUID.randomUUID().toString(), System.currentTimeMillis() + 2_000);
    assertThat(api.post(api.http, "/v1/push", fresh, push(u())).statusCode()).isEqualTo(202);
  }

  @Test
  void tenantBlockedIs403() throws Exception {
    cfg("t8", null, KillSwitches.newBuilder().setTenantBlocked(true).build());
    await().atMost(Duration.ofSeconds(5)).until(() -> api.ctx.getBean(ControlState.class).policy("t8").blocked());
    HttpResponse<String> r = api.post(api.http, "/v1/push", api.token("t8", "t8-backend", SEND), push("t8:u1"));
    assertThat(r.statusCode()).isEqualTo(403);
    assertThat(json(r).get("code").asText()).isEqualTo("tenant_blocked");
  }

  @Test
  void rateLimitIs429WithRetryAfter() throws Exception {
    cfg("t9", TenantLimits.newBuilder().setPushRate(1).setPushBurst(1).build(), null);
    await().atMost(Duration.ofSeconds(5)).until(() -> api.ctx.getBean(ControlState.class).policy("t9").pushRate() == 1);
    String token = api.token("t9", "t9-backend", SEND);
    assertThat(api.post(api.http, "/v1/push", token, push("t9:u1")).statusCode()).isEqualTo(202);
    HttpResponse<String> r = api.post(api.http, "/v1/push", token, push("t9:u1"));
    assertThat(r.statusCode()).isEqualTo(429);
    assertThat(r.headers().firstValue("Retry-After")).contains("1");
    assertThat(json(r).get("code").asText()).isEqualTo("rate_limited");
  }

  @Test
  void statusIsAcceptedThenOffline() throws Exception {
    String token = api.token("t1", "t1-backend", SEND);
    HttpResponse<String> r = api.post(api.http, "/v1/push", token, push(u()));
    String id = json(r).get("msgId").asText();
    HttpResponse<String> s = api.get(api.http, "/v1/messages/" + id, token);
    assertThat(s.statusCode()).isEqualTo(200);
    assertThat(json(s).get("state").asText()).isEqualTo("accepted");
    assertThat(json(s).get("receipt").asBoolean()).isFalse();
    PushRecord rec = scenario.awaitRequest(id);
    scenario.produce(PushTopics.offline("c1").name(), rec.getPrincipalId(), OfflineRecord.newBuilder().setRecord(rec)
        .setReason(OfflineRecord.Reason.REASON_NO_SESSION).setAtMs(System.currentTimeMillis()).build(),
        "OfflineRecord", "t1");
    await().atMost(Duration.ofSeconds(15)).until(() -> json(api.get(api.http, "/v1/messages/" + id, token))
        .path("state").asText().equals("offline"));
    JsonNode off = json(api.get(api.http, "/v1/messages/" + id, token));
    assertThat(off.get("offlineReason").asText()).isEqualTo("no_session");
    assertThat(api.get(api.http, "/v1/messages/01UNKNOWN", token).statusCode()).isEqualTo(404);
    assertThat(api.get(api.http, "/v1/messages/" + id, api.token("t2", "t2-backend", SEND)).statusCode())
        .isEqualTo(404);
  }

  @Test
  void deliveredWinsOverOfflineAcrossTopics() throws Exception {
    String token = api.token("t1", "t1-backend", SEND);
    String body = push(u()).replace("\"payload\"", "\"receipt\":true,\"senderPrincipalId\":\"t1:svc\",\"payload\"");
    String id = json(api.post(api.http, "/v1/push", token, body)).get("msgId").asText();
    PushRecord rec = scenario.awaitRequest(id);
    scenario.produce(PushTopics.delivered("c1").name(), "t1:svc", DeliveredRecord.newBuilder().setMsgId(id)
        .setTenantId("t1").setSenderPrincipalId("t1:svc").setPrincipalId(rec.getPrincipalId()).setConnId("C1")
        .setStatus(AckItem.Status.STATUS_ACKED).setAtMs(System.currentTimeMillis()).build(), "DeliveredRecord", "t1");
    await().atMost(Duration.ofSeconds(15)).until(() -> json(api.get(api.http, "/v1/messages/" + id, token))
        .path("state").asText().equals("delivered"));
    scenario.produce(PushTopics.offline("c1").name(), rec.getPrincipalId(), OfflineRecord.newBuilder().setRecord(rec)
        .setReason(OfflineRecord.Reason.REASON_CLOSED).setAttempts(3).build(), "OfflineRecord", "t1");
    Thread.sleep(1_500);
    JsonNode st = json(api.get(api.http, "/v1/messages/" + id, token));
    assertThat(st.get("state").asText()).isEqualTo("delivered");
    assertThat(st.get("deliveredConnections").asLong()).isEqualTo(1);
    assertThat(st.get("lastStatus").asText()).isEqualTo("acked");
  }

  @Test
  void adminBumpNeedsAdminScope() throws Exception {
    String body = "{\"slots\":[1,2]}";
    assertThat(api.post(api.mgmt, "/admin/v1/seqepochs:bump", api.token("t1", "t1-backend", "admin"), body)
        .statusCode()).isEqualTo(403);
    HttpResponse<String> ok = api.post(api.mgmt, "/admin/v1/seqepochs:bump",
        api.token("pangate", "pangate-admin", "admin"), body);
    assertThat(ok.statusCode()).as(ok.body()).isEqualTo(200);
    assertThat(json(ok).get("bumped").asInt()).isEqualTo(2);
    assertThat(api.post(api.http, "/admin/v1/seqepochs:bump", api.token("pangate", "pangate-admin", "admin"), body)
        .statusCode()).isEqualTo(404);   // admin is only on the management port
  }
}
