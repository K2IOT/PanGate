package com.pangate.devtools.issuer;

import com.nimbusds.jose.jwk.ECKey;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.util.JSONObjectUtils;
import com.pangate.devtools.jwt.JwtMinter;
import com.pangate.devtools.jwt.TokenRequest;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

/** Dev token issuer: serves the public JWKS and mints connect tokens (stand-in for the Auth service). */
public final class IssuerServer {

  private final ECKey key;
  private final JwtMinter minter;
  private final HttpServer http;

  public IssuerServer(ECKey key, int port) {
    this.key = key;
    this.minter = new JwtMinter(key, "pangate-devtools");
    try {
      this.http = HttpServer.create(new InetSocketAddress(port), 64);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    http.createContext("/.well-known/jwks.json", this::jwks);
    http.createContext("/tokens", this::tokens);
    http.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
  }

  public void start() {
    http.start();
  }

  public void stop() {
    http.stop(0);
  }

  public int port() {
    return http.getAddress().getPort();
  }

  private void jwks(HttpExchange ex) throws IOException {
    if (!"GET".equals(ex.getRequestMethod())) {
      send(ex, 405, "{}");
      return;
    }
    send(ex, 200, new JWKSet(key.toPublicJWK()).toString(true));
  }

  private void tokens(HttpExchange ex) throws IOException {
    if (!"POST".equals(ex.getRequestMethod())) {
      send(ex, 405, "{}");
      return;
    }
    try {
      Map<String, Object> body = JSONObjectUtils.parse(new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
      String tenant = (String) body.get("tenant");
      String principal = (String) body.get("principal");
      String device = (String) body.get("device");
      if (tenant == null || principal == null || device == null) {
        send(ex, 400, "{\"error\":\"tenant, principal and device are required\"}");
        return;
      }
      @SuppressWarnings("unchecked")
      List<String> topics = body.get("topics") instanceof List<?> l ? (List<String>) l : List.of();
      long ttlSec = body.get("ttlSec") instanceof Number n ? n.longValue() : 600L;
      String token = minter.mint(new TokenRequest(tenant, principal, device, topics, Duration.ofSeconds(ttlSec)), Instant.now());
      send(ex, 200, JSONObjectUtils.toJSONString(Map.of("token", token)));
    } catch (java.text.ParseException | ClassCastException e) {
      send(ex, 400, "{\"error\":\"invalid JSON body\"}");
    }
  }

  private static void send(HttpExchange ex, int status, String body) throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    ex.getResponseHeaders().set("Content-Type", "application/json");
    ex.sendResponseHeaders(status, bytes.length);
    try (var out = ex.getResponseBody()) {
      out.write(bytes);
    }
  }
}
