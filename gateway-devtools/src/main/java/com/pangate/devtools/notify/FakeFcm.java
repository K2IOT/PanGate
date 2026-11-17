package com.pangate.devtools.notify;

import com.nimbusds.jose.util.JSONObjectUtils;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;

/**
 * FCM HTTP v1 stand-in (offline notifications spec §7): {@code POST /token} hands out the OAuth2 access token
 * {@code fake}; {@code POST /v1/projects/<p>/messages:send} requires it and records the message. Tokens starting with
 * {@code dead} answer 404 UNREGISTERED.
 */
public final class FakeFcm implements AutoCloseable {

  private final HttpServer server;
  private final ProviderLog log = new ProviderLog();

  private FakeFcm(HttpServer server) {
    this.server = server;
  }

  public static FakeFcm start(int port) throws IOException {
    HttpServer s = HttpServer.create(new InetSocketAddress(port), 0);
    FakeFcm f = new FakeFcm(s);
    s.createContext("/token", f::token);
    s.createContext("/v1/projects/", f::send);
    s.createContext("/received", f.log::serve);
    s.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
    s.start();
    return f;
  }

  public int port() {
    return server.getAddress().getPort();
  }

  private void token(HttpExchange x) throws IOException {
    String body = ProviderLog.body(x);
    if (!body.contains("assertion=")) {
      ProviderLog.respond(x, 400, "{\"error\":\"invalid_request\"}", Map.of());
      return;
    }
    ProviderLog.respond(x, 200, "{\"access_token\":\"fake\",\"expires_in\":3600,\"token_type\":\"Bearer\"}",
        Map.of());
  }

  private void send(HttpExchange x) throws IOException {
    String path = x.getRequestURI().getPath();
    String body = ProviderLog.body(x);
    if (!path.endsWith("/messages:send")) {
      ProviderLog.respond(x, 404, "{\"error\":{\"status\":\"NOT_FOUND\"}}", Map.of());
      return;
    }
    if (!"Bearer fake".equals(x.getRequestHeaders().getFirst("Authorization"))) {
      ProviderLog.respond(x, 401, "{\"error\":{\"code\":401,\"status\":\"UNAUTHENTICATED\"}}", Map.of());
      return;
    }
    String token;
    try {
      token = JSONObjectUtils.getString(JSONObjectUtils.getJSONObject(JSONObjectUtils.parse(body), "message"),
          "token");
    } catch (Exception e) {
      token = null;
    }
    if (token == null) {
      ProviderLog.respond(x, 400, "{\"error\":{\"code\":400,\"status\":\"INVALID_ARGUMENT\"}}", Map.of());
      return;
    }
    if (token.startsWith("dead")) {
      ProviderLog.respond(x, 404, "{\"error\":{\"code\":404,\"status\":\"NOT_FOUND\",\"details\":[{\"@type\":"
          + "\"type.googleapis.com/google.firebase.fcm.v1.FcmError\",\"errorCode\":\"UNREGISTERED\"}]}}", Map.of());
      return;
    }
    log.add(token, Map.of(), body);
    String project = path.substring("/v1/projects/".length(), path.indexOf("/messages:send"));
    ProviderLog.respond(x, 200, "{\"name\":\"projects/" + project + "/messages/" + UUID.randomUUID() + "\"}",
        Map.of());
  }

  @Override
  public void close() {
    server.stop(0);
  }
}
