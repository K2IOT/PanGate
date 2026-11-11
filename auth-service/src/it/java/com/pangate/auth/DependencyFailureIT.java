package com.pangate.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.pangate.auth.application.ManageClients;
import com.pangate.auth.application.ManageTenants;
import com.pangate.auth.config.AuthProperties;
import com.pangate.auth.domain.Scope;
import com.pangate.auth.support.AuthApp;
import com.pangate.auth.support.Containers;
import com.pangate.auth.support.Http;
import com.pangate.auth.support.Tokens;
import com.pangate.gateway.auth.JwtResult;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/** Spec §6 and §12 item 6. */
class DependencyFailureIT {

  @Test
  void postgresDownKeepsJwksAndFailsTokenWith503() {
    try (var pg = new PostgreSQLContainer<>(DockerImageName.parse("postgres:16.4")).withDatabaseName("pangate")
        .withUsername("pangate").withPassword("pangate")) {
      pg.start();
      try (var ctx = AuthApp.start(pg.getJdbcUrl())) {
        int port = AuthApp.port(ctx);
        int admin = AuthApp.adminPort(ctx);
        ctx.getBean(ManageTenants.class).ensure("t1");
        var client = ctx.getBean(ManageClients.class).create("t1", "dep", EnumSet.of(Scope.TOKENS_MINT));
        Http.Res minted = Http.postJson(port, "/v1/tokens", Tokens.api(ctx, client.clientId(), "t1",
            Set.of(Scope.TOKENS_MINT)), "{\"principal\":\"t1:u1\",\"device\":\"d1\"}");
        assertThat(minted.status()).isEqualTo(200);
        String connect = minted.body().replaceAll(".*\"token\":\"([^\"]+)\".*", "$1");
        assertThat(Http.get(admin, "/ready", null).status()).isEqualTo(200);

        pg.stop();

        assertThat(Http.get(port, "/.well-known/jwks.json", null).status()).isEqualTo(200);
        var verifier = ConnectTokenIT.gatewayVerifier(port, ctx.getBean(AuthProperties.class).issuer());
        assertThat(verifier.verify(connect, Instant.now().getEpochSecond())).isInstanceOf(JwtResult.Verified.class);

        String basic = Base64.getEncoder().encodeToString((client.clientId() + ":" + client.plainSecret())
            .getBytes(StandardCharsets.UTF_8));
        Http.Res token = Http.send("POST", port, "/oauth2/token", null, "application/x-www-form-urlencoded",
            "grant_type=client_credentials", Map.of("Authorization", "Basic " + basic));
        assertThat(token.status()).isEqualTo(503);
        assertThat(token.header("Retry-After")).isEqualTo("5");
        assertThat(token.body()).contains("\"error\":\"temporarily_unavailable\"");

        Http.Res mint = Http.postJson(port, "/v1/tokens", Tokens.api(ctx, client.clientId(), "t1",
            Set.of(Scope.TOKENS_MINT)), "{\"principal\":\"t1:u1\",\"device\":\"d1\"}");
        assertThat(mint.status()).isEqualTo(503);
        assertThat(mint.header("Retry-After")).isEqualTo("5");
        assertThat(mint.body()).contains("\"code\":\"db_unavailable\"");

        await().atMost(Duration.ofSeconds(20)).until(() -> Http.get(admin, "/ready", null).status() == 503);
        assertThat(Http.get(admin, "/live", null).status()).isEqualTo(200);
      }
    }
  }

  @Test
  void kafkaDownKeepsReady200() {
    try (var ctx = AuthApp.start(Containers.freshDatabaseOnce("dep_kafka"))) {
      var k = Containers.kafka();
      k.getDockerClient().pauseContainerCmd(k.getContainerId()).exec();
      try {
        ctx.getBean(ManageTenants.class).ensure("t1");
        Http.Res r = Http.postJson(AuthApp.port(ctx), "/v1/revocations", Tokens.api(ctx, "t1-backend", "t1",
            Set.of(Scope.SESSIONS_REVOKE)), "{\"kind\":\"principal\",\"principal\":\"t1:dep\"}");
        assertThat(r.status()).isEqualTo(202);
        for (int i = 0; i < 5; i++) {
          assertThat(Http.get(AuthApp.adminPort(ctx), "/ready", null).status()).isEqualTo(200);
          sleep(500);
        }
        assertThat(Http.get(AuthApp.adminPort(ctx), "/actuator/prometheus", null).body())
            .contains("auth_outbox_pending").contains("auth_outbox_oldest_age_seconds");
      } finally {
        k.getDockerClient().unpauseContainerCmd(k.getContainerId()).exec();
      }
    }
  }

  private static void sleep(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
