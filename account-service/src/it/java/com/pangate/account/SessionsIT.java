package com.pangate.account;

import static com.pangate.account.support.Http.call;
import static com.pangate.account.support.Http.unique;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.nimbusds.jwt.SignedJWT;
import com.pangate.account.support.AccountIT;
import com.pangate.account.support.Accounts;
import com.pangate.account.support.Accounts.Device;
import com.pangate.account.support.ControlTopic;
import com.pangate.account.support.Http.Res;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.web.server.LocalServerPort;

@AccountIT
class SessionsIT {

  @LocalServerPort int port;

  @Test
  void exchangeGivesAConnectTokenForThisDevice() throws Exception {
    Device d = Accounts.register(port, unique("ex"));
    Res r = call(port, "POST", "/v1/sessions/token", d.refreshToken(), null);
    assertThat(r.status()).isEqualTo(200);
    assertThat(r.body().get("expiresAt").asText()).isNotBlank();
    var claims = SignedJWT.parse(r.body().get("connectToken").asText()).getJWTClaimsSet();
    assertThat(claims.getSubject()).isEqualTo("chat:" + d.userId());
    assertThat(claims.getStringClaim("device")).isEqualTo(d.deviceId());
    assertThat(call(port, "POST", "/v1/sessions/token", "rt_" + "x".repeat(43), null).status()).isEqualTo(401);
  }

  @Test
  void logoutOneDeviceRevokesOnlyIt() {
    String name = unique("lo");
    Device d1 = Accounts.register(port, name);
    Device d2 = Accounts.login(port, name, "secret-pw-1");
    String t2 = d2.connectToken(port);
    assertThat(call(port, "DELETE", "/v1/sessions/" + d1.deviceId(), t2, null).status()).isEqualTo(204);
    String key = "revoke:d:chat:" + d1.userId() + "/" + d1.deviceId();
    assertThat(ControlTopic.hasKey("gw.control", key, Duration.ofSeconds(20))).isTrue();
    assertThat(ControlTopic.await("gw.control", rs -> false, Duration.ofSeconds(1)).stream()
        .noneMatch(r -> ("revoke:p:chat:" + d1.userId()).equals(r.key()))).isTrue();
    Res again = call(port, "POST", "/v1/sessions/token", d1.refreshToken(), null);
    assertThat(again.status()).isEqualTo(401);
    assertThat(again.body().get("code").asText()).isEqualTo("invalid_session");
    assertThat(call(port, "POST", "/v1/sessions/token", d2.refreshToken(), null).status()).isEqualTo(200);
    assertThat(call(port, "DELETE", "/v1/sessions/" + d1.deviceId(), t2, null).status()).isEqualTo(404);
  }

  @Test
  void logoutOthersKeepsTheCaller() {
    String name = unique("lx");
    Accounts.register(port, name);
    Device d2 = Accounts.login(port, name, "secret-pw-1");
    Accounts.login(port, name, "secret-pw-1");
    String t2 = d2.connectToken(port);
    assertThat(call(port, "GET", "/v1/sessions", t2, null).body().get("sessions")).hasSize(3);
    assertThat(call(port, "DELETE", "/v1/sessions?others=true", t2, null).status()).isEqualTo(204);
    Res list = call(port, "GET", "/v1/sessions", t2, null);
    assertThat(list.body().get("sessions")).singleElement().satisfies(s -> {
      assertThat(s.get("deviceId").asText()).isEqualTo(d2.deviceId());
      assertThat(s.get("current").asBoolean()).isTrue();
      assertThat(s.get("deviceName").asText()).isEqualTo("it");
    });
  }

  /** Review Focus 4 (needs Task 8's password change): the device that changes the password stays connected. */
  @Test
  @org.junit.jupiter.api.Disabled("enabled in Task 8 with POST /v1/me/password")
  void passwordChangeKeepsTheCurrentDeviceConnected() throws Exception {
    String name = unique("pw");
    Device d1 = Accounts.register(port, name);
    Device d2 = Accounts.login(port, name, "secret-pw-1");
    String t2 = d2.connectToken(port);
    Res r = call(port, "POST", "/v1/me/password", t2, Map.of("currentPassword", "secret-pw-1",
        "newPassword", "secret-pw-2"));
    assertThat(r.status()).as(r.raw().body()).isEqualTo(204);
    assertThat(call(port, "POST", "/v1/sessions/token", d1.refreshToken(), null).status()).isEqualTo(401);
    List<Future<Integer>> both = new ArrayList<>();
    try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
      for (int i = 0; i < 2; i++) {
        both.add(pool.submit(() -> call(port, "POST", "/v1/sessions/token", d2.refreshToken(), null).status()));
      }
    }
    for (Future<Integer> f : both) {
      assertThat(f.get()).isEqualTo(200);
    }
    assertThat(call(port, "GET", "/v1/sessions", t2, null).status()).isEqualTo(200);
  }

  @Test
  void connectTokenOfLoggedOutDeviceIsRejectedByAccountApi() {
    Device d1 = Accounts.register(port, unique("rj"));
    String t1 = d1.connectToken(port);
    assertThat(call(port, "GET", "/v1/sessions", t1, null).status()).isEqualTo(200);
    assertThat(call(port, "DELETE", "/v1/sessions/" + d1.deviceId(), t1, null).status()).isEqualTo(204);
    await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
        assertThat(call(port, "GET", "/v1/sessions", t1, null).status()).isEqualTo(401));
    assertThat(call(port, "GET", "/v1/sessions", null, null).status()).isEqualTo(401);
  }
}
