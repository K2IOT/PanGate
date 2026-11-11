package com.pangate.devtools.auth;

import com.nimbusds.jose.util.JSONObjectUtils;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;

/**
 * Client of auth-service for dev tools: client credentials with a secret from the seed file
 * ({@code .dev/auth/clients.json}), then {@code /v1/tokens} and {@code /v1/revocations}.
 */
public final class AuthClient {

  static final Duration TIMEOUT = Duration.ofSeconds(10);
  static final long REFRESH_BEFORE_SEC = 60;

  /** Non-2xx answer from Auth. */
  public static final class AuthException extends RuntimeException {
    AuthException(String what, int status, String body) {
      super(what + " answered " + status + ": " + body);
    }
  }

  private final URI base;
  private final Path clientFile;
  private final String clientId;
  private final LongSupplier nowSec;
  private final HttpClient http = HttpClient.newBuilder().connectTimeout(TIMEOUT).build();
  private String apiToken;
  private long apiTokenRefreshAt;

  public AuthClient(URI authUrl, Path clientFile, String clientId) {
    this(authUrl, clientFile, clientId, () -> System.currentTimeMillis() / 1000);
  }

  public AuthClient(URI authUrl, Path clientFile, String clientId, LongSupplier nowSec) {
    this.base = authUrl;
    this.clientFile = clientFile;
    this.clientId = clientId;
    this.nowSec = nowSec;
  }

  /** An API token, reused until 60 s before it expires. */
  public synchronized String apiToken() {
    long now = nowSec.getAsLong();
    if (apiToken == null || now >= apiTokenRefreshAt) {
      String basic = Base64.getEncoder().encodeToString((enc(clientId) + ":" + enc(secret()))
          .getBytes(StandardCharsets.UTF_8));
      Map<String, Object> res = send("token endpoint", HttpRequest.newBuilder(base.resolve("/oauth2/token"))
          .header("Authorization", "Basic " + basic)
          .header("Content-Type", "application/x-www-form-urlencoded")
          .POST(HttpRequest.BodyPublishers.ofString("grant_type=client_credentials")));
      apiToken = res.get("access_token").toString();
      apiTokenRefreshAt = now + ((Number) res.get("expires_in")).longValue() - REFRESH_BEFORE_SEC;
    }
    return apiToken;
  }

  public String connectToken(String principal, String device, List<String> topics) {
    Map<String, Object> body = new LinkedHashMap<>();
    body.put("principal", principal);
    body.put("device", device);
    body.put("topics", topics);
    return postJson("/v1/tokens", body).get("token").toString();
  }

  public void revokePrincipal(String principal) {
    postJson("/v1/revocations", Map.of("kind", "principal", "principal", principal));
  }

  private Map<String, Object> postJson(String path, Map<String, Object> body) {
    return send(path, HttpRequest.newBuilder(base.resolve(path))
        .header("Authorization", "Bearer " + apiToken())
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(JSONObjectUtils.toJSONString(body))));
  }

  private Map<String, Object> send(String what, HttpRequest.Builder req) {
    try {
      HttpResponse<String> res = http.send(req.timeout(TIMEOUT).build(), HttpResponse.BodyHandlers.ofString());
      if (res.statusCode() / 100 != 2) {
        throw new AuthException(what, res.statusCode(), res.body());
      }
      return JSONObjectUtils.parse(res.body());
    } catch (IOException e) {
      throw new UncheckedIOException(what + " failed", e);
    } catch (java.text.ParseException e) {
      throw new IllegalStateException(what + " returned invalid JSON", e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }

  private String secret() {
    try {
      Object s = JSONObjectUtils.parse(Files.readString(clientFile)).get(clientId);
      if (s == null) {
        throw new IllegalStateException("client " + clientId + " is not in " + clientFile);
      }
      return s.toString();
    } catch (IOException e) {
      throw new UncheckedIOException("cannot read " + clientFile, e);
    } catch (java.text.ParseException e) {
      throw new IllegalStateException(clientFile + " is not JSON", e);
    }
  }

  private static String enc(String s) {
    return URLEncoder.encode(s, StandardCharsets.UTF_8);
  }
}
