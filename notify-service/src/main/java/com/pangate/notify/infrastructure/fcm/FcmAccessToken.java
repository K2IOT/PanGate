package com.pangate.notify.infrastructure.fcm;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.PrivateKey;
import java.time.Duration;
import java.util.Date;
import java.util.function.LongSupplier;

/**
 * OAuth2 access token of an FCM service account (spec §5.5): a JWT RS256 assertion exchanged at the token URI
 * ({@code urn:ietf:params:oauth:grant-type:jwt-bearer}), cached until 60 s before it expires.
 */
public final class FcmAccessToken {

  static final String SCOPE = "https://www.googleapis.com/auth/firebase.messaging";
  private static final ObjectMapper JSON = new ObjectMapper();

  private final HttpClient http;
  private final String clientEmail;
  private final PrivateKey key;
  private final String tokenUri;
  private final LongSupplier wallMs;
  private String token;            // guarded by this
  private long validUntilMs;       // guarded by this

  public FcmAccessToken(HttpClient http, String clientEmail, PrivateKey key, String tokenUri, LongSupplier wallMs) {
    this.http = http;
    this.clientEmail = clientEmail;
    this.key = key;
    this.tokenUri = tokenUri;
    this.wallMs = wallMs;
  }

  public synchronized String token() throws Exception {
    long now = wallMs.getAsLong();
    if (token != null && now < validUntilMs) {
      return token;
    }
    SignedJWT assertion = new SignedJWT(new JWSHeader.Builder(JWSAlgorithm.RS256).build(),
        new JWTClaimsSet.Builder().issuer(clientEmail).audience(tokenUri).claim("scope", SCOPE)
            .issueTime(new Date(now)).expirationTime(new Date(now + 3_600_000)).build());
    assertion.sign(new RSASSASigner(key));
    String form = "grant_type=" + URLEncoder.encode("urn:ietf:params:oauth:grant-type:jwt-bearer",
        StandardCharsets.UTF_8) + "&assertion=" + assertion.serialize();
    HttpResponse<String> res = http.send(HttpRequest.newBuilder(URI.create(tokenUri)).timeout(Duration.ofSeconds(10))
        .header("Content-Type", "application/x-www-form-urlencoded")
        .POST(HttpRequest.BodyPublishers.ofString(form)).build(), HttpResponse.BodyHandlers.ofString());
    if (res.statusCode() != 200) {
      throw new IllegalStateException("token endpoint answered " + res.statusCode());
    }
    JsonNode b = JSON.readTree(res.body());
    token = b.get("access_token").asText();
    validUntilMs = now + Math.max(0, b.path("expires_in").asLong(3600) - 60) * 1000;
    return token;
  }

  /** FCM refused the token: fetch a new one on the next call. */
  public synchronized void forget() {
    token = null;
  }
}
