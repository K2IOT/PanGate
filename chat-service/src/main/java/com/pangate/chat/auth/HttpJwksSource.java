package com.pangate.chat.auth;

import com.nimbusds.jose.jwk.JWKSet;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/** {@code GET <issuer>/.well-known/jwks.json}. */
public final class HttpJwksSource implements JwksSource {

  private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
  private final URI uri;

  public HttpJwksSource(String uri) {
    this.uri = URI.create(uri);
  }

  @Override
  public JWKSet fetch() throws Exception {
    HttpResponse<String> res = http.send(HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(3)).build(),
        HttpResponse.BodyHandlers.ofString());
    if (res.statusCode() != 200) {
      throw new IllegalStateException("JWKS answered " + res.statusCode());
    }
    return JWKSet.parse(res.body());
  }
}
