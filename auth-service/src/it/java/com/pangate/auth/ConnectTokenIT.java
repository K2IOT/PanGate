package com.pangate.auth;

import static org.assertj.core.api.Assertions.assertThat;

import com.nimbusds.jose.jwk.JWKSet;
import com.pangate.auth.config.AuthProperties;
import com.pangate.auth.domain.Scope;
import com.pangate.auth.support.AuthIT;
import com.pangate.auth.support.Http;
import com.pangate.auth.support.Tokens;
import com.pangate.gateway.auth.JwksCache;
import com.pangate.gateway.auth.JwksSource;
import com.pangate.gateway.auth.JwtResult;
import com.pangate.gateway.auth.JwtVerifier;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.ApplicationContext;

@AuthIT
class ConnectTokenIT {

  @Autowired ApplicationContext ctx;
  @Autowired AuthProperties props;
  @LocalServerPort int port;

  private String mintToken() {
    return Tokens.api(ctx, "t1-backend", "t1", Set.of(Scope.TOKENS_MINT));
  }

  /** The gateway's JWKS cache fed over HTTP from this Auth instance. */
  static JwtVerifier gatewayVerifier(int port, String issuer) {
    JwksSource http = etag -> {
      Http.Res r = Http.get(port, "/.well-known/jwks.json", null);
      try {
        return new JwksSource.Updated(JWKSet.parse(r.body()), null, Duration.ofMinutes(5));
      } catch (java.text.ParseException e) {
        throw new java.io.IOException(e);
      }
    };
    var cache = new JwksCache(http, Executors.newSingleThreadScheduledExecutor(), Runnable::run,
        Duration.ofMinutes(5), System::currentTimeMillis);
    cache.refresh().join();
    return new JwtVerifier(cache, "pangate-gw", issuer, Duration.ofSeconds(30));
  }

  @Test
  void mintedTokenPassesTheGatewayVerifier() {
    ensureTenantT1();
    Http.Res r = Http.postJson(port, "/v1/tokens", mintToken(),
        "{\"principal\":\"t1:u123\",\"device\":\"d1\",\"topics\":[\"t1:news.*\"],\"ttlSec\":300}");
    assertThat(r.status()).isEqualTo(200);
    String token = r.body().replaceAll(".*\"token\":\"([^\"]+)\".*", "$1");
    String expiresAt = r.body().replaceAll(".*\"expiresAt\":\"([^\"]+)\".*", "$1");
    assertThat(Instant.parse(expiresAt)).isBetween(Instant.now().plusSeconds(290), Instant.now().plusSeconds(301));

    JwtResult v = gatewayVerifier(port, props.issuer()).verify(token, Instant.now().getEpochSecond());
    assertThat(v).isInstanceOf(JwtResult.Verified.class);
    var c = ((JwtResult.Verified) v).claims();
    assertThat(c.principal()).isEqualTo("t1:u123");
    assertThat(c.topics()).containsExactly("t1:news.*");
  }

  @Test
  void missingScopeIs403() {
    ensureTenantT1();
    Http.Res r = Http.postJson(port, "/v1/tokens", Tokens.api(ctx, "t1-backend", "t1", Set.of(Scope.PUSH_SEND)),
        "{\"principal\":\"t1:u\",\"device\":\"d\"}");
    assertThat(r.status()).isEqualTo(403);
    assertThat(r.body()).contains("\"code\":\"missing_scope\"");
  }

  @Test
  void limitViolationsAre400ProblemJsonWithCode() {
    ensureTenantT1();
    Map<String, String> cases = Map.of(
        "{\"principal\":\"t2:u\",\"device\":\"d\"}", "principal_outside_tenant",
        "{\"principal\":\"t1:" + "a".repeat(254) + "\",\"device\":\"d\"}", "principal_too_long",
        "{\"principal\":\"t1:u\",\"device\":\"\"}", "device_invalid",
        "{\"principal\":\"t1:u\",\"device\":\"d\",\"topics\":[\"t1:a\",\"t1:a\",\"t1:a\",\"t1:a\",\"t1:a\",\"t1:a\","
            + "\"t1:a\",\"t1:a\",\"t1:a\"]}", "too_many_topics",
        "{\"principal\":\"t1:u\",\"device\":\"d\",\"topics\":[\"t2:a\"]}", "topic_outside_tenant",
        "{\"principal\":\"t1:u\",\"device\":\"d\",\"topics\":[\"t1:" + "a".repeat(62) + "\"]}",
        "topic_pattern_too_long",
        "{\"principal\":\"t1:u\",\"device\":\"d\",\"ttlSec\":601}", "ttl_out_of_range",
        "{not json", "malformed_request");
    cases.forEach((body, code) -> {
      Http.Res r = Http.postJson(port, "/v1/tokens", mintToken(), body);
      assertThat(r.status()).as(code).isEqualTo(400);
      assertThat(r.header("Content-Type")).as(code).startsWith("application/problem+json");
      assertThat(r.body()).as(code).contains("\"code\":\"" + code + "\"");
    });
  }

  @Test
  void disabledTenantIs403() {
    var tenants = ctx.getBean(com.pangate.auth.application.ManageTenants.class);
    tenants.ensure("t9");
    tenants.disable("t9");
    Http.Res r = Http.postJson(port, "/v1/tokens", Tokens.api(ctx, "t9-backend", "t9", Set.of(Scope.TOKENS_MINT)),
        "{\"principal\":\"t9:u\",\"device\":\"d\"}");
    assertThat(r.status()).isEqualTo(403);
    assertThat(r.body()).contains("tenant_disabled");
  }

  private void ensureTenantT1() {
    ctx.getBean(com.pangate.auth.application.ManageTenants.class).ensure("t1");
  }
}
