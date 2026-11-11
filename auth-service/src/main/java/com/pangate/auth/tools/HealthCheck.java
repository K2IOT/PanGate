package com.pangate.auth.tools;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/** Container healthcheck for the distroless image (no curl): exit 0 iff GET args[0] answers 200. */
public final class HealthCheck {

  private HealthCheck() {}

  public static void main(String[] args) {
    try {
      HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
      HttpResponse<Void> res = client.send(HttpRequest.newBuilder(URI.create(args[0]))
          .timeout(Duration.ofSeconds(2)).build(), HttpResponse.BodyHandlers.discarding());
      System.exit(res.statusCode() == 200 ? 0 : 1);
    } catch (Exception e) {
      System.exit(1);
    }
  }
}
