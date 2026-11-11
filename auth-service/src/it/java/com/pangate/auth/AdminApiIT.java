package com.pangate.auth;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.auth.config.AdminConnector;
import com.pangate.auth.domain.Scope;
import com.pangate.auth.support.AuthIT;
import com.pangate.auth.support.Http;
import com.pangate.auth.support.Tokens;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.ApplicationContext;

@AuthIT
@org.junit.jupiter.api.extension.ExtendWith(org.springframework.boot.test.system.OutputCaptureExtension.class)
class AdminApiIT {

  @Autowired ApplicationContext ctx;
  @Autowired AdminConnector admin;
  @LocalServerPort int publicPort;

  String adminToken;
  String tenant;

  @BeforeEach
  void setUp() {
    adminToken = Tokens.admin(ctx);
    tenant = "a" + UUID.randomUUID().toString().substring(0, 8);
  }

  private Http.Res createTenant(String token) {
    return Http.postJson(admin.port(), "/admin/v1/tenants", token, "{\"id\":\"" + tenant + "\"}");
  }

  @Test
  void adminRoutesNeedAdminScope() {
    String customer = Tokens.api(ctx, "t1-backend", "t1", Set.of(Scope.TOKENS_MINT, Scope.SESSIONS_REVOKE));
    Http.Res forbidden = createTenant(customer);
    assertThat(forbidden.status()).isEqualTo(403);
    assertThat(forbidden.body()).contains("\"code\":\"missing_scope\"");

    Http.Res noToken = createTenant(null);
    assertThat(noToken.status()).isEqualTo(401);
    assertThat(noToken.header("WWW-Authenticate")).isEqualTo("Bearer error=\"invalid_token\"");
    assertThat(createTenant("garbage").status()).isEqualTo(401);

    Http.Res ok = createTenant(adminToken);
    assertThat(ok.status()).isEqualTo(201);
    assertThat(ok.body()).contains("\"id\":\"" + tenant + "\"").contains("\"status\":\"active\"");
    assertThat(createTenant(adminToken).status()).isEqualTo(409);
  }

  @Test
  void adminRoutesAreNotOnThePublicPort() {
    assertThat(Http.postJson(publicPort, "/admin/v1/tenants", adminToken, "{\"id\":\"x\"}").status()).isEqualTo(404);
    assertThat(Http.get(publicPort, "/actuator/prometheus", null).status()).isEqualTo(404);
    assertThat(Http.get(publicPort, "/ready", null).status()).isEqualTo(404);
    assertThat(Http.get(admin.port(), "/.well-known/jwks.json", null).status()).isEqualTo(404);
    assertThat(Http.get(admin.port(), "/live", null).status()).isEqualTo(200);
    assertThat(Http.get(admin.port(), "/ready", null).status()).isEqualTo(200);
    assertThat(Http.get(publicPort, "/.well-known/jwks.json", null).status()).isEqualTo(200);
  }

  @Test
  void clientLifecycleAndListNeverReturnsSecretsOrHashes() {
    createTenant(adminToken);
    Http.Res created = Http.postJson(admin.port(), "/admin/v1/tenants/" + tenant + "/clients", adminToken,
        "{\"name\":\"backend\",\"scopes\":[\"tokens:mint\",\"push:send\"]}");
    assertThat(created.status()).isEqualTo(201);
    assertThat(created.body()).contains("\"clientId\":\"" + tenant + "-backend\"").contains("\"secret\":\"");

    Http.Res list = Http.get(admin.port(), "/admin/v1/tenants/" + tenant + "/clients", adminToken);
    assertThat(list.status()).isEqualTo(200);
    assertThat(list.body()).contains(tenant + "-backend").doesNotContain("secret").doesNotContain("$argon2")
        .doesNotContain("hash");

    String id = tenant + "-backend";
    Http.Res rotated = Http.postJson(admin.port(), "/admin/v1/clients/" + id + ":rotate-secret", adminToken, null);
    assertThat(rotated.status()).isEqualTo(200);
    assertThat(rotated.body()).contains("\"secret\":\"");
    assertThat(Http.postJson(admin.port(), "/admin/v1/clients/" + id + ":finish-rotation", adminToken, null).status())
        .isEqualTo(200);
    assertThat(Http.postJson(admin.port(), "/admin/v1/clients/" + id + ":disable", adminToken, null).status())
        .isEqualTo(200);
    assertThat(Http.postJson(admin.port(), "/admin/v1/tenants/" + tenant + ":disable", adminToken, null).body())
        .contains("\"status\":\"disabled\"");
    assertThat(Http.postJson(admin.port(), "/admin/v1/tenants/" + tenant + ":enable", adminToken, null).body())
        .contains("\"status\":\"active\"");
    Http.Res adminForCustomer = Http.postJson(admin.port(), "/admin/v1/tenants/" + tenant + "/clients", adminToken,
        "{\"name\":\"ops\",\"scopes\":[\"admin\"]}");
    assertThat(adminForCustomer.status()).isEqualTo(400);
    assertThat(adminForCustomer.body()).contains("scope_not_allowed");
  }

  @Test
  void unknownTenantIs404ProblemJson() {
    Http.Res r = Http.get(admin.port(), "/admin/v1/tenants/nope-" + tenant, adminToken);
    assertThat(r.status()).isEqualTo(404);
    assertThat(r.header("Content-Type")).startsWith("application/problem+json");
    assertThat(r.body()).contains("\"code\":\"tenant_not_found\"").contains("\"status\":404");
  }

  @Test
  void rotateNowPublishesAPendingKey() {
    Http.Res r = Http.postJson(admin.port(), "/admin/v1/keys:rotate-now", adminToken, null);
    assertThat(r.status()).isEqualTo(202);
    assertThat(Http.get(publicPort, "/.well-known/jwks.json", null).body().split("\"kid\"").length - 1)
        .isGreaterThanOrEqualTo(2);
  }

  @Test
  void malformedJsonIs400() {
    Http.Res r = Http.postJson(admin.port(), "/admin/v1/tenants", adminToken, "{not json");
    assertThat(r.status()).isEqualTo(400);
    assertThat(r.body()).contains("malformed_request");
  }

  @Test
  void deniedCallsAre403BeforeTheBodyIsJudgedAndAreAudited(
      org.springframework.boot.test.system.CapturedOutput out) {
    var registry = ctx.getBean(io.micrometer.core.instrument.MeterRegistry.class);
    String customer = Tokens.api(ctx, "t1-backend", "t1", Set.of(Scope.TOKENS_MINT));
    Http.Res r = Http.postJson(admin.port(), "/admin/v1/tenants/t1/clients", customer,
        "{\"name\":\"x\",\"scopes\":[\"bogus\"]}");
    assertThat(r.status()).isEqualTo(403);
    var denied = registry.find("auth.admin.actions").tag("action", "client.create").tag("result", "denied").counter();
    assertThat(denied).isNotNull();
    assertThat(denied.count()).isGreaterThanOrEqualTo(1);
    assertThat(out.getAll()).contains("admin_action=client.create").contains("actor=t1-backend")
        .contains("result=denied");
  }
}
