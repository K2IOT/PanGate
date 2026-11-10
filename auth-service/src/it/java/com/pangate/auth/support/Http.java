package com.pangate.auth.support;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;

/** Tiny JSON/form HTTP client for ITs. */
public final class Http {

  private static final HttpClient CLIENT = HttpClient.newHttpClient();

  public record Res(int status, String body, java.net.http.HttpHeaders headers) {
    public String header(String name) {
      return headers.firstValue(name).orElse(null);
    }
  }

  private Http() {}

  public static Res send(String method, int port, String path, String bearer, String contentType, String body,
      Map<String, String> headers) {
    var b = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path));
    if (bearer != null) {
      b.header("Authorization", "Bearer " + bearer);
    }
    if (contentType != null) {
      b.header("Content-Type", contentType);
    }
    headers.forEach(b::header);
    b.method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body));
    try {
      HttpResponse<String> r = CLIENT.send(b.build(), HttpResponse.BodyHandlers.ofString());
      return new Res(r.statusCode(), r.body(), r.headers());
    } catch (java.io.IOException e) {
      throw new java.io.UncheckedIOException(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }

  public static Res get(int port, String path, String bearer) {
    return send("GET", port, path, bearer, null, null, Map.of());
  }

  public static Res postJson(int port, String path, String bearer, String json) {
    return send("POST", port, path, bearer, "application/json", json, Map.of());
  }
}
