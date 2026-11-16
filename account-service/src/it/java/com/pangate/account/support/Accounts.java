package com.pangate.account.support;

import static com.pangate.account.support.Http.call;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;

/** Register / log in / exchange over HTTP for ITs. */
public final class Accounts {

  public record Device(String userId, String deviceId, String refreshToken) {
    public String connectToken(int port) {
      Http.Res r = call(port, "POST", "/v1/sessions/token", refreshToken, null);
      assertThat(r.status()).as(r.raw().body()).isEqualTo(200);
      return r.body().get("connectToken").asText();
    }
  }

  private Accounts() {}

  static Device device(JsonNode b) {
    return new Device(b.get("userId").asText(), b.get("deviceId").asText(), b.get("refreshToken").asText());
  }

  /** A fresh account; returns its first device. Username and password: {@code username}, "secret-pw-1". */
  public static Device register(int port, String username) {
    Http.Res r = call(port, "POST", "/v1/accounts", null,
        Http.registration(username, username.toLowerCase() + "@x.vn", null));
    assertThat(r.status()).as(r.raw().body()).isEqualTo(201);
    return device(r.body());
  }

  public static Device login(int port, String login, String password) {
    Http.Res r = call(port, "POST", "/v1/sessions", null, Http.login(login, password));
    assertThat(r.status()).as(r.raw().body()).isEqualTo(201);
    return device(r.body());
  }
}
