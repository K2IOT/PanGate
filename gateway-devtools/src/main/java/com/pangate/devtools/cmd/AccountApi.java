package com.pangate.devtools.cmd;

import com.nimbusds.jose.util.JSONObjectUtils;
import com.pangate.devtools.auth.AuthClient;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;

/**
 * account-service over HTTP for devtools (account spec §4, §5): register, log in, exchange a refresh token for a
 * connect token, log a device out; and, with the chat-admin client of auth-service, lock and unlock.
 */
final class AccountApi {

  static final Duration T = Duration.ofSeconds(10);
  static final String PASSWORD = "smoke-pw-1";

  record Account(String username, String userId, String deviceId, String refreshToken) {}

  private final URI base;
  private final URI admin;
  private final AuthClient adminAuth;
  private final HttpClient http = HttpClient.newHttpClient();

  /** {@code admin} and {@code adminAuth} may be null when no admin call is made. */
  AccountApi(URI base, URI admin, AuthClient adminAuth) {
    this.base = base;
    this.admin = admin;
    this.adminAuth = adminAuth;
  }

  HttpResponse<String> send(URI root, String method, String path, String bearer, String body) {
    try {
      HttpRequest.Builder b = HttpRequest.newBuilder(root.resolve(path)).timeout(T);
      if (bearer != null) {
        b.header("Authorization", "Bearer " + bearer);
      }
      if (body != null) {
        b.header("Content-Type", "application/json");
      }
      b.method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body));
      return http.send(b.build(), HttpResponse.BodyHandlers.ofString());
    } catch (Exception e) {
      throw new IllegalStateException(method + " " + path + ": " + e, e);
    }
  }

  static Map<String, Object> ok(HttpResponse<String> r, int status) {
    if (r.statusCode() != status) {
      throw new IllegalStateException(r.request().method() + " " + r.uri().getPath() + " answered " + r.statusCode()
          + ": " + r.body());
    }
    try {
      return r.body().isEmpty() ? Map.of() : JSONObjectUtils.parse(r.body());
    } catch (java.text.ParseException e) {
      throw new IllegalStateException(e);
    }
  }

  static Account account(String username, Map<String, Object> m) {
    return new Account(username, m.get("userId").toString(), m.get("deviceId").toString(),
        m.get("refreshToken").toString());
  }

  /** Registers {@code username} with email {@code <username>@smoke.invalid} and password {@link #PASSWORD}. */
  Account register(String username) {
    String body = "{\"username\":\"" + username + "\",\"password\":\"" + PASSWORD + "\",\"email\":\"" + username
        + "@smoke.invalid\",\"deviceName\":\"smoke\",\"platform\":\"other\"}";
    return account(username, ok(send(base, "POST", "/v1/accounts", null, body), 201));
  }

  HttpResponse<String> loginResponse(String username) {
    return send(base, "POST", "/v1/sessions", null, "{\"login\":\"" + username + "\",\"password\":\"" + PASSWORD
        + "\",\"deviceName\":\"smoke\",\"platform\":\"other\"}");
  }

  Account login(String username) {
    return account(username, ok(loginResponse(username), 201));
  }

  String connectToken(String refreshToken) {
    return ok(send(base, "POST", "/v1/sessions/token", refreshToken, null), 200).get("connectToken").toString();
  }

  void logout(String connectToken, String deviceId) {
    ok(send(base, "DELETE", "/v1/sessions/" + deviceId, connectToken, null), 204);
  }

  void lock(String userId) {
    ok(send(admin, "POST", "/admin/v1/accounts/" + userId + ":lock", adminAuth.apiToken(),
        "{\"reason\":\"smoke\"}"), 204);
  }

  void unlock(String userId) {
    ok(send(admin, "POST", "/admin/v1/accounts/" + userId + ":unlock", adminAuth.apiToken(),
        "{\"reason\":\"smoke\"}"), 204);
  }
}
