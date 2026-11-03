package com.pangate.devtools.issuer;

import static org.assertj.core.api.Assertions.assertThat;

import com.nimbusds.jose.crypto.ECDSAVerifier;
import com.nimbusds.jose.jwk.ECKey;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.util.JSONObjectUtils;
import com.nimbusds.jwt.SignedJWT;
import com.pangate.devtools.keys.DevKeys;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class IssuerServerTest {

  @TempDir Path dir;
  private ECKey key;
  private IssuerServer server;
  private final HttpClient http = HttpClient.newHttpClient();

  @BeforeEach
  void start() throws Exception {
    key = DevKeys.loadOrCreateEs256(dir.resolve("es256.pem"));
    server = new IssuerServer(key, 0);
    server.start();
  }

  @AfterEach
  void stop() {
    server.stop();
  }

  @Test
  void servesPublicJwks() throws Exception {
    HttpResponse<String> res = http.send(
        HttpRequest.newBuilder(base("/.well-known/jwks.json")).build(), HttpResponse.BodyHandlers.ofString());

    assertThat(res.statusCode()).isEqualTo(200);
    JWKSet set = JWKSet.parse(res.body());
    assertThat(set.getKeys()).singleElement().satisfies(k -> {
      assertThat(k.getKeyID()).isEqualTo(key.getKeyID());
      assertThat(k.isPrivate()).isFalse();
    });
  }

  @Test
  void mintsTokens() throws Exception {
    HttpResponse<String> res = http.send(HttpRequest.newBuilder(base("/tokens"))
        .POST(HttpRequest.BodyPublishers.ofString("{\"tenant\":\"t1\",\"principal\":\"t1:u1\",\"device\":\"d1\"}"))
        .header("Content-Type", "application/json").build(), HttpResponse.BodyHandlers.ofString());

    assertThat(res.statusCode()).isEqualTo(200);
    SignedJWT jwt = SignedJWT.parse(JSONObjectUtils.parse(res.body()).get("token").toString());
    assertThat(jwt.verify(new ECDSAVerifier(key.toPublicJWK()))).isTrue();
    assertThat(jwt.getJWTClaimsSet().getSubject()).isEqualTo("t1:u1");
    long ttl = jwt.getJWTClaimsSet().getExpirationTime().getTime() - jwt.getJWTClaimsSet().getNotBeforeTime().getTime();
    assertThat(ttl).isEqualTo(600_000L);
  }

  @Test
  void rejectsTokenRequestWithoutPrincipal() throws Exception {
    HttpResponse<String> res = http.send(HttpRequest.newBuilder(base("/tokens"))
        .POST(HttpRequest.BodyPublishers.ofString("{\"tenant\":\"t1\"}")).build(), HttpResponse.BodyHandlers.ofString());

    assertThat(res.statusCode()).isEqualTo(400);
  }

  private URI base(String path) {
    return URI.create("http://127.0.0.1:" + server.port() + path);
  }
}
