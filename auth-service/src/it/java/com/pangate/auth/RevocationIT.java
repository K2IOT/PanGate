package com.pangate.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.pangate.auth.application.ManageClients;
import com.pangate.auth.application.ManageTenants;
import com.pangate.auth.domain.Scope;
import com.pangate.auth.support.AuthApp;
import com.pangate.auth.support.ControlTopic;
import com.pangate.auth.support.Containers;
import com.pangate.auth.support.Http;
import com.pangate.auth.support.Tokens;
import com.pangate.contracts.v1.ControlEvent;
import com.pangate.starter.kafka.PangateHeaders;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.ConfigurableApplicationContext;

class RevocationIT {

  private final List<ConfigurableApplicationContext> pods = new ArrayList<>();
  private final String user = "t1:u" + UUID.randomUUID().toString().substring(0, 8);

  @AfterEach
  void close() {
    unpause();
    pods.forEach(ConfigurableApplicationContext::close);
  }

  private ConfigurableApplicationContext pod(String db, String... props) {
    var ctx = AuthApp.start(Containers.freshDatabaseOnce(db), props);
    pods.add(ctx);
    ctx.getBean(ManageTenants.class).ensure("t1");
    return ctx;
  }

  private static String revokeToken(ConfigurableApplicationContext ctx) {
    return Tokens.api(ctx, "t1-backend", "t1", Set.of(Scope.SESSIONS_REVOKE));
  }

  private static Http.Res revoke(ConfigurableApplicationContext ctx, String json) {
    return Http.postJson(AuthApp.port(ctx), "/v1/revocations", revokeToken(ctx), json);
  }

  private static List<ConsumerRecord<String, byte[]>> recordsFor(String keyPrefix) {
    return ControlTopic.read("gw.control").stream().filter(r -> r.key().startsWith(keyPrefix)).toList();
  }

  private static void pause() {
    var k = Containers.kafka();
    k.getDockerClient().pauseContainerCmd(k.getContainerId()).exec();
  }

  private static void unpause() {
    var k = Containers.kafka();
    var state = k.getDockerClient().inspectContainerCmd(k.getContainerId()).exec().getState();
    if (Boolean.TRUE.equals(state.getPaused())) {
      k.getDockerClient().unpauseContainerCmd(k.getContainerId()).exec();
    }
  }

  @Test
  void revocationReachesGwControlWithKeyAndExpiry() throws Exception {
    var ctx = pod("rev_basic");
    Http.Res r = revoke(ctx, "{\"kind\":\"principal\",\"principal\":\"" + user + "\"}");
    assertThat(r.status()).isEqualTo(202);
    assertThat(r.body()).contains("\"controlKey\":\"revoke:p:" + user + "\"").contains("\"expiresAt\"")
        .contains("\"id\"");

    await().atMost(Duration.ofSeconds(20)).until(() -> !recordsFor("revoke:p:" + user).isEmpty());
    var rec = recordsFor("revoke:p:" + user).getFirst();
    ControlEvent e = ControlEvent.parseFrom(rec.value());
    assertThat(e.getRevoke().getPrincipalId()).isEqualTo(user);
    assertThat(Instant.ofEpochMilli(e.getRevoke().getExpiresAtMs()))
        .isBetween(Instant.now().plus(Duration.ofHours(23)), Instant.now().plus(Duration.ofHours(24)));
    assertThat(PangateHeaders.read(rec.headers(), PangateHeaders.TENANT_ID)).hasValue("t1");
    assertThat(PangateHeaders.read(rec.headers(), PangateHeaders.CONTENT_TYPE))
        .hasValue("application/x-protobuf; schema=pangate.v1.ControlEvent");

    assertThat(revoke(ctx, "{\"kind\":\"principal\",\"principal\":\"t2:x\"}").status()).isEqualTo(403);
    assertThat(revoke(ctx, "{\"kind\":\"bogus\"}").body()).contains("kind_invalid");
    assertThat(revoke(ctx, "{\"kind\":\"client\",\"clientId\":\"t1-backend\"}").body()).contains("kind_invalid");
    assertThat(Http.postJson(AuthApp.port(ctx), "/v1/revocations",
        Tokens.api(ctx, "t1-backend", "t1", Set.of(Scope.TOKENS_MINT)), "{\"kind\":\"jti\",\"jti\":\"x\"}").status())
        .isEqualTo(403);
  }

  @Test
  void kafkaDownStillReturns202AndDeliversInOrderLater() {
    var ctx = pod("rev_order");
    revoke(ctx, "{\"kind\":\"principal\",\"principal\":\"" + user + "\"}");
    await().atMost(Duration.ofSeconds(20)).until(() -> recordsFor("revoke:").stream()
        .anyMatch(r -> r.key().equals("revoke:p:" + user)));  // producer connected before the pause
    pause();
    List<String> keys = List.of("revoke:d:" + user + "/d1", "revoke:d:" + user + "/d2", "revoke:d:" + user + "/d3");
    for (String d : List.of("d1", "d2", "d3")) {
      assertThat(revoke(ctx, "{\"kind\":\"device\",\"principal\":\"" + user + "\",\"device\":\"" + d + "\"}")
          .status()).isEqualTo(202);
    }
    unpause();
    await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> assertThat(recordsFor("revoke:d:" + user)
        .stream().map(ConsumerRecord::key).distinct().toList()).isEqualTo(keys));
  }

  @Test
  void outboxSurvivesARestart() {
    var first = pod("rev_restart");
    pause();
    assertThat(revoke(first, "{\"kind\":\"jti\",\"jti\":\"" + user + "\"}").status()).isEqualTo(202);
    first.close();
    pods.remove(first);
    unpause();
    pod("rev_restart");  // same database, new process: the unsent row is still there
    await().atMost(Duration.ofSeconds(60)).until(() -> !recordsFor("revoke:j:" + user).isEmpty());
  }

  @Test
  void expiredRevocationIsTombstoned() {
    var ctx = pod("rev_expiry", "auth.revocation-ttl=2s", "auth.expiry-interval=500ms");
    revoke(ctx, "{\"kind\":\"principal\",\"principal\":\"" + user + "\"}");
    await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
      var recs = recordsFor("revoke:p:" + user);
      assertThat(recs).hasSize(2);
      assertThat(recs.get(0).value()).isNotNull();
      assertThat(recs.get(1).value()).isNull();
    });
  }

  @Test
  void disablingAClientRevokesItsTokensAtOnce() {
    var ctx = pod("rev_client");
    var created = ctx.getBean(ManageClients.class).create("t1", "temp" + user.substring(4),
        EnumSet.of(Scope.TOKENS_MINT));
    String basic = Base64.getEncoder().encodeToString((created.clientId() + ":" + created.plainSecret())
        .getBytes(StandardCharsets.UTF_8));
    Http.Res tok = Http.send("POST", AuthApp.port(ctx), "/oauth2/token", null, "application/x-www-form-urlencoded",
        "grant_type=client_credentials", Map.of("Authorization", "Basic " + basic));
    String access = tok.body().replaceAll(".*\"access_token\":\"([^\"]+)\".*", "$1");
    String body = "{\"principal\":\"t1:u\",\"device\":\"d\"}";
    assertThat(Http.postJson(AuthApp.port(ctx), "/v1/tokens", access, body).status()).isEqualTo(200);

    Http.Res disabled = Http.postJson(AuthApp.adminPort(ctx), "/admin/v1/clients/" + created.clientId() + ":disable",
        Tokens.admin(ctx), null);
    assertThat(disabled.status()).isEqualTo(200);

    await().atMost(Duration.ofSeconds(10)).until(() ->
        Http.postJson(AuthApp.port(ctx), "/v1/tokens", access, body).status() == 401);
    await().atMost(Duration.ofSeconds(20)).until(() -> !recordsFor("revoke:c:" + created.clientId()).isEmpty());
  }

  @Test
  void revokedJtiTokenIsRejectedByTheApi() {
    var ctx = pod("rev_jti");
    String jtiToken = revokeToken(ctx);
    String jti = jtiOf(jtiToken);
    assertThat(Http.postJson(AuthApp.port(ctx), "/v1/revocations", jtiToken,
        "{\"kind\":\"jti\",\"jti\":\"" + jti + "\"}").status()).isEqualTo(202);
    await().atMost(Duration.ofSeconds(15)).until(() -> Http.postJson(AuthApp.port(ctx), "/v1/revocations", jtiToken,
        "{\"kind\":\"jti\",\"jti\":\"other\"}").status() == 401);
  }

  private static String jtiOf(String token) {
    try {
      return com.nimbusds.jwt.SignedJWT.parse(token).getJWTClaimsSet().getJWTID();
    } catch (java.text.ParseException e) {
      throw new IllegalStateException(e);
    }
  }

  @Test
  void reRevokeExtendsOneRowKeepsItsCreatorAndPublishesAgain() {
    var ctx = pod("rev_again");
    var jdbc = ctx.getBean(org.springframework.jdbc.core.simple.JdbcClient.class);
    String body = "{\"kind\":\"principal\",\"principal\":\"" + user + "\"}";
    Http.Res first = revoke(ctx, body);
    Instant firstExp = Instant.parse(first.body().replaceAll(".*\"expiresAt\":\"([^\"]+)\".*", "$1"));
    Http.Res second = Http.postJson(AuthApp.port(ctx), "/v1/revocations", Tokens.api(ctx, "t1-other", "t1",
        Set.of(Scope.SESSIONS_REVOKE)), body);
    assertThat(second.status()).isEqualTo(202);
    assertThat(second.body().replaceAll(".*\"id\":([0-9]+).*", "$1"))
        .isEqualTo(first.body().replaceAll(".*\"id\":([0-9]+).*", "$1"));
    var row = jdbc.sql("SELECT count(*), max(created_by), max(expires_at) FROM auth.revocations WHERE control_key = :k")
        .param("k", "revoke:p:" + user).query((rs, i) -> List.of(rs.getString(1), rs.getString(2),
            rs.getTimestamp(3).toInstant().toString())).single();
    assertThat(row.get(0)).isEqualTo("1");
    assertThat(row.get(1)).isEqualTo("t1-backend");  // the first actor is kept
    assertThat(Instant.parse(row.get(2))).isAfterOrEqualTo(firstExp.truncatedTo(java.time.temporal.ChronoUnit.MILLIS));
    assertThat(jdbc.sql("SELECT count(*) FROM auth.outbox WHERE record_key = :k").param("k", "revoke:p:" + user)
        .query(Long.class).single()).isEqualTo(2);
  }
}
