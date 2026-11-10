package com.pangate.auth;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.auth.application.ManageClients;
import com.pangate.auth.application.ManageTenants;
import com.pangate.auth.domain.Scope;
import com.pangate.auth.support.AuthIT;
import com.pangate.auth.support.Http;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.EnumSet;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.server.LocalServerPort;

@AuthIT
class ClientCredentialsIT {

  static final String FORM = "application/x-www-form-urlencoded";

  @Autowired ManageTenants tenants;
  @Autowired ManageClients clients;
  @LocalServerPort int port;

  String clientId;
  String secret;

  @BeforeEach
  void client() {
    String t = "c" + UUID.randomUUID().toString().substring(0, 8);
    tenants.create(t);
    var c = clients.create(t, "my_backend", EnumSet.of(Scope.TOKENS_MINT, Scope.SESSIONS_REVOKE));
    clientId = c.clientId();
    secret = c.plainSecret();
  }

  static String enc(String s) {
    return URLEncoder.encode(s, StandardCharsets.UTF_8);
  }

  private Http.Res token(String body, String basicUser, String basicPass) {
    Map<String, String> h = basicUser == null ? Map.of() : Map.of("Authorization", "Basic " + Base64.getEncoder()
        .encodeToString((enc(basicUser) + ":" + enc(basicPass)).getBytes(StandardCharsets.UTF_8)));
    return Http.send("POST", port, "/oauth2/token", null, FORM, body, h);
  }

  @Test
  void basicAuthWithPercentEncodedCredentials() {
    Http.Res r = token("grant_type=client_credentials", clientId, secret);
    assertThat(r.status()).isEqualTo(200);
    assertThat(r.body()).contains("\"token_type\":\"Bearer\"").contains("\"expires_in\":900")
        .contains("\"scope\":\"tokens:mint sessions:revoke\"").contains("\"access_token\":\"");
    assertThat(r.header("Cache-Control")).isEqualTo("no-store");

    // RFC 6749 §2.3.1: both parts are form-urlencoded before Base64, so "%2D" must decode to "-"
    String raw = clientId.replace("-", "%2D") + ":" + secret.replace("-", "%2D").replace("_", "%5F");
    Http.Res encoded = Http.send("POST", port, "/oauth2/token", null, FORM, "grant_type=client_credentials",
        Map.of("Authorization", "Basic " + Base64.getEncoder().encodeToString(raw.getBytes(StandardCharsets.UTF_8))));
    assertThat(encoded.status()).isEqualTo(200);

    Http.Res wrong = token("grant_type=client_credentials", clientId, secret + "+");
    assertThat(wrong.status()).isEqualTo(401);
    assertThat(wrong.header("WWW-Authenticate")).isEqualTo("Basic realm=\"pangate\"");
    assertThat(wrong.body()).contains("\"error\":\"invalid_client\"");
  }

  @Test
  void formCredentialsWork() {
    Http.Res r = token("grant_type=client_credentials&scope=tokens%3Amint&client_id=" + enc(clientId)
        + "&client_secret=" + enc(secret), null, null);
    assertThat(r.status()).isEqualTo(200);
    assertThat(r.body()).contains("\"scope\":\"tokens:mint\"");
  }

  @Test
  void bothCredentialStylesIsInvalidRequest() {
    Http.Res r = token("grant_type=client_credentials&client_id=" + enc(clientId) + "&client_secret=" + enc(secret),
        clientId, secret);
    assertThat(r.status()).isEqualTo(400);
    assertThat(r.body()).contains("\"error\":\"invalid_request\"");
    assertThat(token("grant_type=client_credentials", null, null).body()).contains("invalid_request");
  }

  @Test
  void wrongGrantTypeIsUnsupportedGrantType() {
    Http.Res r = token("grant_type=password", clientId, secret);
    assertThat(r.status()).isEqualTo(400);
    assertThat(r.body()).contains("\"error\":\"unsupported_grant_type\"");
  }

  @Test
  void scopeOutsideAssignmentIsInvalidScope() {
    Http.Res r = token("grant_type=client_credentials&scope=push%3Asend", clientId, secret);
    assertThat(r.status()).isEqualTo(400);
    assertThat(r.body()).contains("\"error\":\"invalid_scope\"");
  }

  @Test
  void issuedTokenAuthenticatesApiCalls() {
    Http.Res r = token("grant_type=client_credentials", clientId, secret);
    String access = r.body().replaceAll(".*\"access_token\":\"([^\"]+)\".*", "$1");
    // /v1/tokens does not exist yet; the filter accepts the token and the router answers 404, not 401
    assertThat(Http.postJson(port, "/v1/nothing", access, "{}").status()).isEqualTo(404);
    assertThat(Http.postJson(port, "/v1/nothing", "x", "{}").status()).isEqualTo(401);
  }

  @Test
  void credentialsInTheQueryStringAreRefused() {
    Http.Res r = Http.send("POST", port, "/oauth2/token?client_id=" + enc(clientId) + "&client_secret=" + enc(secret),
        null, FORM, "grant_type=client_credentials", Map.of());
    assertThat(r.status()).isEqualTo(400);
    assertThat(r.body()).contains("\"error\":\"invalid_request\"");
  }
}
