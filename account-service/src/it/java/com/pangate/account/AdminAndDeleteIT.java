package com.pangate.account;

import static com.pangate.account.support.Http.call;
import static com.pangate.account.support.Http.unique;
import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.account.config.AdminConnector;
import com.pangate.account.support.AccountIT;
import com.pangate.account.support.Accounts;
import com.pangate.account.support.Accounts.Device;
import com.pangate.account.support.ApiTokens;
import com.pangate.account.support.ControlTopic;
import com.pangate.account.support.Http;
import com.pangate.account.support.Http.Res;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.server.LocalServerPort;

@AccountIT
class AdminAndDeleteIT {

  @LocalServerPort int port;
  @Autowired AdminConnector admin;

  String adminToken() {
    return ApiTokens.of("chat-admin", "accounts:admin");
  }

  Res adm(String method, String path, Object body) {
    return call(admin.port(), method, path, adminToken(), body);
  }

  @Test
  void adminNeedsTheScope() {
    Device d = Accounts.register(port, unique("sc"));
    assertThat(call(admin.port(), "GET", "/admin/v1/accounts/" + d.userId(), null, null).status()).isEqualTo(401);
    String backend = ApiTokens.of("chat-backend", "tokens:mint");
    assertThat(call(admin.port(), "GET", "/admin/v1/accounts/" + d.userId(), backend, null).status())
        .isEqualTo(403);
    Res ok = adm("GET", "/admin/v1/accounts/" + d.userId(), null);
    assertThat(ok.status()).as(ok.raw().body()).isEqualTo(200);
    assertThat(ok.body().get("status").asText()).isEqualTo("active");
    assertThat(ok.body().get("liveSessions").asInt()).isEqualTo(1);
    assertThat(call(port, "GET", "/admin/v1/accounts/" + d.userId(), adminToken(), null).status()).isEqualTo(404);
    assertThat(adm("POST", "/admin/v1/accounts/" + d.userId() + ":lock", Map.of()).status()).isEqualTo(400);
  }

  @Test
  void lockThenUnlock() {
    String name = unique("lk");
    Device d = Accounts.register(port, name);
    Res lock = adm("POST", "/admin/v1/accounts/" + d.userId() + ":lock", Map.of("reason", "spam"));
    assertThat(lock.status()).as(lock.raw().body()).isEqualTo(204);
    Res login = call(port, "POST", "/v1/sessions", null, Http.login(name, "secret-pw-1"));
    assertThat(login.status()).isEqualTo(403);
    assertThat(login.body().get("code").asText()).isEqualTo("account_locked");
    assertThat(ControlTopic.hasKey("gw.control", "revoke:d:chat:" + d.userId() + "/" + d.deviceId(),
        Duration.ofSeconds(20))).isTrue();
    assertThat(ControlTopic.await("gw.control", rs -> false, Duration.ofSeconds(1)).stream()
        .noneMatch(r -> ("revoke:p:chat:" + d.userId()).equals(r.key()))).isTrue();
    assertThat(adm("POST", "/admin/v1/accounts/" + d.userId() + ":unlock", Map.of("reason", "ok")).status())
        .isEqualTo(204);
    Device again = Accounts.login(port, name, "secret-pw-1");
    assertThat(again.connectToken(port)).isNotBlank();
    Res view = adm("GET", "/admin/v1/accounts?username=" + name, null);
    assertThat(view.body().get("actions")).hasSize(2);
  }

  /** Review Focus 3. */
  @Test
  void deletedIdentifiersAreReusableAndOldIdStaysDeleted() {
    String name = unique("dl");
    Res reg = call(port, "POST", "/v1/accounts", null, Http.registration(name, name + "@x.vn", null));
    String oldId = reg.body().get("userId").asText();
    assertThat(adm("DELETE", "/admin/v1/accounts/" + oldId, Map.of("reason", "asked")).status()).isEqualTo(204);
    assertThat(ControlTopic.hasKey("gw.control", "revoke:p:chat:" + oldId, Duration.ofSeconds(20))).isTrue();
    Res again = call(port, "POST", "/v1/accounts", null, Http.registration(name, name + "@x.vn", null));
    assertThat(again.status()).as(again.raw().body()).isEqualTo(201);
    String newId = again.body().get("userId").asText();
    assertThat(newId).isNotEqualTo(oldId);
    String t = Accounts.login(port, name, "secret-pw-1").connectToken(port);
    Res old = call(port, "GET", "/v1/users/" + oldId, t, null);
    assertThat(old.body().get("deleted").asBoolean()).isTrue();
    assertThat(old.body().has("username")).isFalse();
    assertThat(call(port, "GET", "/v1/users?username=" + name, t, null).body().get("userId").asText())
        .isEqualTo(newId);
  }

  @Test
  void selfDeleteNeedsThePassword() {
    String name = unique("sd");
    Device d = Accounts.register(port, name);
    String t = d.connectToken(port);
    assertThat(call(port, "DELETE", "/v1/me", t, Map.of("password", "wrong-pw-9")).status()).isEqualTo(401);
    assertThat(call(port, "DELETE", "/v1/me", t, Map.of("password", "secret-pw-1")).status()).isEqualTo(204);
    assertThat(call(port, "POST", "/v1/sessions", null, Http.login(name, "secret-pw-1")).status()).isEqualTo(401);
    assertThat(adm("GET", "/admin/v1/accounts/" + d.userId(), null).body().get("status").asText())
        .isEqualTo("deleted");
  }
}
