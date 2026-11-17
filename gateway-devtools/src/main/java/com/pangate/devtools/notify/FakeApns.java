package com.pangate.devtools.notify;

import com.nimbusds.jwt.SignedJWT;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

/**
 * APNs stand-in (offline notifications spec §7): {@code POST /3/device/<token>} as Apple takes it. Checks the provider
 * token (an ES256 JWT with {@code kid} and {@code iss}) and {@code apns-topic}; records the request. Tokens starting
 * with {@code dead} answer 410 Unregistered; with {@code busy}, 429 the first time. Plain HTTP/1.1: a client asking for
 * HTTP/2 over cleartext falls back to it, and real APNs (TLS + ALPN h2) is out of reach in tests anyway.
 */
public final class FakeApns implements AutoCloseable {

  private final HttpServer server;
  private final ProviderLog log = new ProviderLog();
  private final Set<String> busySeen = ConcurrentHashMap.newKeySet();

  private FakeApns(HttpServer server) {
    this.server = server;
  }

  public static FakeApns start(int port) throws IOException {
    HttpServer s = HttpServer.create(new InetSocketAddress(port), 0);
    FakeApns f = new FakeApns(s);
    s.createContext("/3/device/", f::send);
    s.createContext("/received", f.log::serve);
    s.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
    s.start();
    return f;
  }

  public int port() {
    return server.getAddress().getPort();
  }

  static boolean validProviderToken(String auth) {
    if (auth == null || !auth.regionMatches(true, 0, "bearer ", 0, 7)) {
      return false;
    }
    try {
      SignedJWT j = SignedJWT.parse(auth.substring(7).trim());
      return j.getHeader().getKeyID() != null && j.getJWTClaimsSet().getIssuer() != null;
    } catch (Exception e) {
      return false;
    }
  }

  private void send(HttpExchange x) throws IOException {
    String token = x.getRequestURI().getPath().substring("/3/device/".length());
    String body = ProviderLog.body(x);
    if (!"POST".equals(x.getRequestMethod())) {
      ProviderLog.respond(x, 405, "{\"reason\":\"MethodNotAllowed\"}", Map.of());
      return;
    }
    if (!validProviderToken(x.getRequestHeaders().getFirst("authorization"))) {
      ProviderLog.respond(x, 403, "{\"reason\":\"InvalidProviderToken\"}", Map.of());
      return;
    }
    if (x.getRequestHeaders().getFirst("apns-topic") == null) {
      ProviderLog.respond(x, 400, "{\"reason\":\"MissingTopic\"}", Map.of());
      return;
    }
    if (token.startsWith("dead")) {
      ProviderLog.respond(x, 410, "{\"reason\":\"Unregistered\",\"timestamp\":" + System.currentTimeMillis() + "}",
          Map.of());
      return;
    }
    if (token.startsWith("busy") && busySeen.add(token)) {
      ProviderLog.respond(x, 429, "{\"reason\":\"TooManyRequests\"}", Map.of("retry-after", "1"));
      return;
    }
    Map<String, String> h = new LinkedHashMap<>();
    for (String k : new String[] {"apns-topic", "apns-push-type", "apns-priority", "apns-expiration",
        "apns-collapse-id"}) {
      String v = x.getRequestHeaders().getFirst(k);
      if (v != null) {
        h.put(k, v);
      }
    }
    log.add(token, h, body);
    ProviderLog.respond(x, 200, "", Map.of("apns-id", java.util.UUID.randomUUID().toString()));
  }

  @Override
  public void close() {
    server.stop(0);
  }
}
