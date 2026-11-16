package com.pangate.account;

import static com.pangate.account.support.Http.call;
import static com.pangate.account.support.Http.login;
import static com.pangate.account.support.Http.phone;
import static com.pangate.account.support.Http.registration;
import static com.pangate.account.support.Http.unique;
import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.account.support.AccountIT;
import com.pangate.account.support.Http.Res;
import com.pangate.contracts.v1.AccountState;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.jdbc.core.simple.JdbcClient;

@AccountIT
class RegisterLoginIT {

  @LocalServerPort int port;
  @Autowired JdbcClient jdbc;

  @Test
  void registerThenLoginWithEachKind() throws Exception {
    String username = unique("lan");
    String email = unique("m") + "@x.vn";
    String phone = phone();
    Res r = call(port, "POST", "/v1/accounts", null, registration(username, email, phone));
    assertThat(r.status()).as(r.raw().body()).isEqualTo(201);
    String userId = r.body().get("userId").asText();
    assertThat(r.body().get("deviceId").asText()).hasSize(26);
    assertThat(r.body().get("refreshToken").asText()).startsWith("rt_");
    byte[] state = jdbc.sql("SELECT payload FROM account.outbox WHERE kind = 'event' AND record_key = ?")
        .param(userId).query(byte[].class).single();
    assertThat(AccountState.parseFrom(state).getStatus()).isEqualTo(AccountState.Status.STATUS_ACTIVE);
    Set<String> devices = new HashSet<>();
    devices.add(r.body().get("deviceId").asText());
    for (String who : new String[] {email, phone, username}) {
      Res l = call(port, "POST", "/v1/sessions", null, login(who, "secret-pw-1"));
      assertThat(l.status()).as(who + " " + l.raw().body()).isEqualTo(201);
      assertThat(l.body().get("userId").asText()).isEqualTo(userId);
      devices.add(l.body().get("deviceId").asText());
    }
    assertThat(devices).hasSize(4);
  }

  /** Review Focus 1. */
  @Test
  void loginIsCaseAndSpaceInsensitive() {
    String username = unique("Mixed");
    String email = unique("Lan") + "@Mail.com";
    assertThat(call(port, "POST", "/v1/accounts", null, registration(username, " " + email + " ", null)).status())
        .isEqualTo(201);
    assertThat(call(port, "POST", "/v1/sessions", null, login("  " + email.toUpperCase() + "  ", "secret-pw-1"))
        .status()).isEqualTo(201);
    assertThat(call(port, "POST", "/v1/sessions", null, login(username.toUpperCase(), "secret-pw-1")).status())
        .isEqualTo(201);
    Res wrong = call(port, "POST", "/v1/sessions", null, login(username, "secret-pw-2"));
    assertThat(wrong.status()).isEqualTo(401);
    assertThat(wrong.body().get("code").asText()).isEqualTo("invalid_credentials");
    assertThat(wrong.raw().headers().firstValue("Content-Type")).get().asString()
        .startsWith("application/problem+json");
  }

  @Test
  void takenFieldsAre409() {
    String username = unique("taken");
    String email = unique("t") + "@x.vn";
    String phone = phone();
    assertThat(call(port, "POST", "/v1/accounts", null, registration(username, email, phone)).status())
        .isEqualTo(201);
    Res u = call(port, "POST", "/v1/accounts", null, registration(username.toUpperCase(), unique("o") + "@x.vn", null));
    assertThat(u.status()).isEqualTo(409);
    assertThat(u.body().get("code").asText()).isEqualTo("username_taken");
    assertThat(call(port, "POST", "/v1/accounts", null, registration(unique("other"), email, null)).body()
        .get("code").asText()).isEqualTo("email_taken");
    assertThat(call(port, "POST", "/v1/accounts", null, registration(unique("other"), null, phone)).body()
        .get("code").asText()).isEqualTo("phone_taken");
    Res bad = call(port, "POST", "/v1/accounts", null, registration("x", email, null));
    assertThat(bad.status()).isEqualTo(400);
    assertThat(bad.body().get("code").asText()).isEqualTo("invalid_username");
  }

  @Test
  void adminPathIsNotOnThePublicPort() {
    assertThat(call(port, "GET", "/ready", null, null).status()).isEqualTo(404);
  }
}
