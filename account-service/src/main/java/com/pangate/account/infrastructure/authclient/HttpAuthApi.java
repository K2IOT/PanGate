package com.pangate.account.infrastructure.authclient;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pangate.account.application.port.AuthApi;
import com.pangate.contracts.testkit.ChatTopics;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Map;

/**
 * auth-service over HTTP (account spec §2): an API token of the {@code chat-backend} client (client credentials,
 * scope {@code tokens:mint sessions:revoke}), cached until 60 s before it expires, then {@code /v1/tokens} and
 * {@code /v1/revocations}. Every call times out after {@code timeout}.
 */
public final class HttpAuthApi implements AuthApi {

  private static final ObjectMapper JSON = new ObjectMapper();

  private final String baseUrl;
  private final String basic;
  private final Duration timeout;
  private final HttpClient http;
  private volatile String apiToken;
  private volatile Instant apiTokenUntil = Instant.EPOCH;

  public HttpAuthApi(String baseUrl, String clientId, String clientSecret, Duration timeout) {
    this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
    this.basic = "Basic " + Base64.getEncoder().encodeToString((form(clientId) + ":" + form(clientSecret))
        .getBytes(StandardCharsets.UTF_8));
    this.timeout = timeout;
    this.http = HttpClient.newBuilder().connectTimeout(timeout).build();
  }

  static String form(String s) {
    return URLEncoder.encode(s, StandardCharsets.UTF_8);
  }

  private HttpResponse<String> send(HttpRequest r) {
    try {
      return http.send(r, HttpResponse.BodyHandlers.ofString());
    } catch (IOException e) {
      throw new AuthUnavailableException("auth-service did not answer: " + e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AuthUnavailableException("interrupted");
    }
  }

  /**
   * 401 / 403 say account-service's own credentials do not work (secret rotated, client disabled, scope missing):
   * nothing is wrong with the request, so it is retried, never dropped. Only other 4xx reject the request itself.
   */
  private void check(HttpResponse<String> r) {
    int s = r.statusCode();
    if (s == 401 || s == 403) {
      apiTokenUntil = Instant.EPOCH;   // get a new API token next time
      throw new AuthUnavailableException("auth-service refused account-service's API token: " + s);
    }
    if (s == 429 || s >= 500) {
      throw new AuthUnavailableException("auth-service answered " + s);
    }
    if (s >= 400) {
      throw new AuthRejectedException(s);
    }
  }

  /** A lock, not synchronized: a virtual thread waiting for the fetch must not pin its carrier (JDK 21). */
  private final java.util.concurrent.locks.ReentrantLock tokenLock = new java.util.concurrent.locks.ReentrantLock();

  private String apiToken() {
    if (apiToken != null && Instant.now().isBefore(apiTokenUntil)) {
      return apiToken;
    }
    tokenLock.lock();
    try {
      return fetchApiToken();
    } finally {
      tokenLock.unlock();
    }
  }

  private String fetchApiToken() {
    if (apiToken != null && Instant.now().isBefore(apiTokenUntil)) {
      return apiToken;
    }
    HttpResponse<String> r = send(HttpRequest.newBuilder(URI.create(baseUrl + "/oauth2/token")).timeout(timeout)
        .header("Authorization", basic).header("Content-Type", "application/x-www-form-urlencoded")
        .POST(HttpRequest.BodyPublishers.ofString("grant_type=client_credentials&scope="
            + form("tokens:mint sessions:revoke"))).build());
    if (r.statusCode() != 200) {   // our own client credentials: never a reason to drop a revocation
      throw new AuthUnavailableException("no API token from auth-service: " + r.statusCode());
    }
    JsonNode n = read(r.body());
    apiToken = n.get("access_token").asText();
    apiTokenUntil = Instant.now().plusSeconds(Math.max(0, n.get("expires_in").asLong() - 60));
    return apiToken;
  }

  private HttpResponse<String> postJson(String path, String body) {
    HttpResponse<String> r = send(HttpRequest.newBuilder(URI.create(baseUrl + path)).timeout(timeout)
        .header("Authorization", "Bearer " + apiToken()).header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(body)).build());
    check(r);
    return r;
  }

  static JsonNode read(String body) {
    try {
      return JSON.readTree(body);
    } catch (IOException e) {
      throw new AuthUnavailableException("unreadable answer from auth-service");
    }
  }

  @Override
  public ConnectToken mint(String userId, String deviceId) {
    try {
      String body = JSON.writeValueAsString(Map.of("principal", ChatTopics.PRINCIPAL_PREFIX + userId,
          "device", deviceId, "topics", List.of(), "ttlSec", 600));
      JsonNode n = read(postJson("/v1/tokens", body).body());
      return new ConnectToken(n.get("token").asText(), Instant.parse(n.get("expiresAt").asText()));
    } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public void revoke(byte[] revocationJson) {
    postJson("/v1/revocations", new String(revocationJson, StandardCharsets.UTF_8));
  }
}
