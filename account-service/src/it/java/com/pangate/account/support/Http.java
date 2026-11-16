package com.pangate.account.support;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;
import java.util.UUID;

/** JSON over HTTP against a running account-service. */
public final class Http {

  public static final ObjectMapper JSON = new ObjectMapper();
  private static final HttpClient CLIENT = HttpClient.newHttpClient();

  public record Res(int status, JsonNode body, HttpResponse<String> raw) {}

  private Http() {}

  public static Res call(int port, String method, String path, String bearer, Object body) {
    try {
      HttpRequest.Builder b = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path));
      if (bearer != null) {
        b.header("Authorization", "Bearer " + bearer);
      }
      if (body != null) {
        b.header("Content-Type", "application/json");
        b.method(method, HttpRequest.BodyPublishers.ofString(JSON.writeValueAsString(body)));
      } else {
        b.method(method, HttpRequest.BodyPublishers.noBody());
      }
      HttpResponse<String> r = CLIENT.send(b.build(), HttpResponse.BodyHandlers.ofString());
      JsonNode n = r.body() == null || r.body().isBlank() ? JSON.nullNode() : JSON.readTree(r.body());
      return new Res(r.statusCode(), n, r);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  public static String unique(String prefix) {
    return prefix + UUID.randomUUID().toString().replace("-", "").substring(0, 10);
  }

  /** A registration body: username, password "secret-pw-1", email, a phone, device "it"/"other". */
  public static Map<String, Object> registration(String username, String email, String phone) {
    java.util.HashMap<String, Object> m = new java.util.HashMap<>();
    m.put("username", username);
    m.put("password", "secret-pw-1");
    if (email != null) {
      m.put("email", email);
    }
    if (phone != null) {
      m.put("phone", phone);
    }
    m.put("deviceName", "it");
    m.put("platform", "other");
    return m;
  }

  public static Map<String, Object> login(String login, String password) {
    return Map.of("login", login, "password", password, "deviceName", "it", "platform", "other");
  }

  public static String phone() {
    return "+849" + (10_000_000 + (int) (Math.random() * 89_999_999));
  }
}
