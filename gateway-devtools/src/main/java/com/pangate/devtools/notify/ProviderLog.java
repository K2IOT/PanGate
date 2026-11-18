package com.pangate.devtools.notify;

import com.nimbusds.jose.util.JSONObjectUtils;
import com.sun.net.httpserver.HttpExchange;
import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** What a fake provider received, per device token; served as {@code GET /received?token=<t>} (a JSON array). */
final class ProviderLog {

  private final Map<String, List<Map<String, Object>>> byToken = new ConcurrentHashMap<>();

  void add(String token, Map<String, String> headers, String body) {
    Map<String, Object> e = new LinkedHashMap<>();
    e.put("headers", headers);
    e.put("body", body);
    byToken.computeIfAbsent(token, k -> java.util.Collections.synchronizedList(new ArrayList<>())).add(e);
  }

  void serve(HttpExchange x) throws IOException {
    String q = x.getRequestURI().getRawQuery();
    String token = "";
    if (q != null) {
      for (String part : q.split("&")) {
        if (part.startsWith("token=")) {
          token = URLDecoder.decode(part.substring(6), StandardCharsets.UTF_8);
        }
      }
    }
    List<Map<String, Object>> list = byToken.getOrDefault(token, List.of());
    StringBuilder sb = new StringBuilder("[");
    synchronized (list) {
      for (int i = 0; i < list.size(); i++) {
        sb.append(i == 0 ? "" : ",").append(JSONObjectUtils.toJSONString(list.get(i)));
      }
    }
    respond(x, 200, sb.append(']').toString(), Map.of());
  }

  static String body(HttpExchange x) throws IOException {
    return new String(x.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
  }

  static void respond(HttpExchange x, int status, String body, Map<String, String> headers) throws IOException {
    byte[] b = body.getBytes(StandardCharsets.UTF_8);
    x.getResponseHeaders().add("Content-Type", "application/json");
    headers.forEach((k, v) -> x.getResponseHeaders().add(k, v));
    x.sendResponseHeaders(status, b.length == 0 ? -1 : b.length);
    if (b.length > 0) {
      x.getResponseBody().write(b);
    }
    x.close();
  }
}
