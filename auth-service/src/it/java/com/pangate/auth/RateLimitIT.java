package com.pangate.auth;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.auth.application.ManageClients;
import com.pangate.auth.application.ManageTenants;
import com.pangate.auth.domain.Scope;
import com.pangate.auth.support.AuthIT;
import com.pangate.auth.support.Http;
import com.pangate.auth.support.Tokens;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.TestPropertySource;

@AuthIT
@TestPropertySource(properties = {"auth.rate-limits.token.rate=1", "auth.rate-limits.token.burst=5",
    "auth.rate-limits.auth-failures.per-minute=20", "auth.rate-limits.auth-failures.block=2s"})
class RateLimitIT {

  @Autowired ApplicationContext ctx;
  @Autowired ManageTenants tenants;
  @Autowired ManageClients clients;
  @LocalServerPort int port;

  private Http.Res token(String id, String secret) {
    String basic = Base64.getEncoder().encodeToString((id + ":" + secret).getBytes(StandardCharsets.UTF_8));
    return Http.send("POST", port, "/oauth2/token", null, "application/x-www-form-urlencoded",
        "grant_type=client_credentials", Map.of("Authorization", "Basic " + basic));
  }

  private ManageClients.CreatedClient client() {
    tenants.ensure("t1");
    return clients.create("t1", "rl" + UUID.randomUUID().toString().substring(0, 8), EnumSet.of(Scope.TOKENS_MINT));
  }

  @Test
  void tokenEndpointLimitsPerClient() {
    var c = client();
    List<Http.Res> res = new ArrayList<>();
    for (int i = 0; i < 21; i++) {
      res.add(token(c.clientId(), c.plainSecret()));
    }
    var limited = res.stream().filter(r -> r.status() == 429).toList();
    assertThat(limited).isNotEmpty();
    assertThat(Integer.parseInt(limited.getFirst().header("Retry-After"))).isPositive();
    assertThat(res.getFirst().status()).isEqualTo(200);
  }

  @Test
  void repeatedBadSecretsBlockTheIp() throws Exception {
    var c = client();
    for (int i = 0; i < 21; i++) {
      token("t1-nobody-" + i, "bad");
    }
    Http.Res blocked = token(c.clientId(), c.plainSecret());
    assertThat(blocked.status()).isEqualTo(429);
    assertThat(blocked.header("Retry-After")).isNotNull();
    Thread.sleep(2_500);  // block is 2 s in this test
    assertThat(token(c.clientId(), c.plainSecret()).status()).isEqualTo(200);
  }

  @Test
  void oversizedBodyIs413() {
    String big = "{\"principal\":\"t1:u\",\"device\":\"d\",\"pad\":\"" + "x".repeat(17_000) + "\"}";
    Http.Res r = Http.postJson(port, "/v1/tokens", Tokens.api(ctx, "t1-backend", "t1", Set.of(Scope.TOKENS_MINT)),
        big);
    assertThat(r.status()).isEqualTo(413);
    assertThat(r.body()).contains("payload_too_large");
  }

  @Test
  void prometheusIsNotOnThePublicPort() {
    assertThat(Http.get(port, "/actuator/prometheus", null).status()).isEqualTo(404);
  }

  @Test
  void behindTheIngressEachClientAddressIsBlockedOnItsOwn() {
    var c = client();
    for (int i = 0; i < 21; i++) {
      tokenFrom("203.0.113.7", "t1-nobody-" + i, "bad");
    }
    assertThat(tokenFrom("203.0.113.7", c.clientId(), c.plainSecret()).status()).isEqualTo(429);
    assertThat(tokenFrom("203.0.113.8", c.clientId(), c.plainSecret()).status()).isEqualTo(200);
  }

  private Http.Res tokenFrom(String clientAddress, String id, String secret) {
    String basic = Base64.getEncoder().encodeToString((id + ":" + secret).getBytes(StandardCharsets.UTF_8));
    return Http.send("POST", port, "/oauth2/token", null, "application/x-www-form-urlencoded",
        "grant_type=client_credentials", Map.of("Authorization", "Basic " + basic, "X-Forwarded-For", clientAddress));
  }
}
