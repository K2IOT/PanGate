package com.pangate.notify.infrastructure.apns;

import com.pangate.contracts.v1.NotifyRequest;
import com.pangate.notify.application.SendResult;
import com.pangate.notify.application.port.PushProvider;
import com.pangate.notify.domain.PushToken;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/** APNs over HTTP/2 with a provider token (offline notifications spec §5.4, §5.5). */
public final class ApnsProvider implements PushProvider {

  private final HttpClient http;
  private final String productionUrl;
  private final String sandboxUrl;
  private final String bundleId;
  private final ApnsJwt jwt;

  public ApnsProvider(HttpClient http, String productionUrl, String sandboxUrl, String bundleId, ApnsJwt jwt) {
    this.http = http;
    this.productionUrl = productionUrl;
    this.sandboxUrl = sandboxUrl;
    this.bundleId = bundleId;
    this.jwt = jwt;
  }

  @Override
  public CompletableFuture<SendResult> send(PushToken t, NotifyRequest r) {
    String base = "sandbox".equals(t.environment()) ? sandboxUrl : productionUrl;
    HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(base + "/3/device/" + t.token()))
        .timeout(Duration.ofSeconds(10)).header("authorization", "bearer " + jwt.token())
        .POST(HttpRequest.BodyPublishers.ofString(ApnsPayload.body(r)));
    ApnsPayload.headers(r, bundleId).forEach(b::header);
    return http.sendAsync(b.build(), HttpResponse.BodyHandlers.ofString())
        .thenApply(this::result)
        .exceptionally(e -> new SendResult.Transient(0));
  }

  SendResult result(HttpResponse<String> res) {
    int s = res.statusCode();
    String body = res.body() == null ? "" : res.body();
    if (s == 200) {
      return new SendResult.Ok();
    }
    if (s == 410 || (s == 400 && (body.contains("BadDeviceToken") || body.contains("DeviceTokenNotForTopic")))) {
      return new SendResult.TokenDead();
    }
    if (s == 403) {
      if (body.contains("ExpiredProviderToken")) {
        jwt.forget();
        return new SendResult.Transient(0);
      }
      return new SendResult.AuthFailed();
    }
    if (s == 429 || s >= 500) {
      return new SendResult.Transient(retryAfterMs(res));
    }
    return new SendResult.Rejected(s + " " + body);
  }

  public static long retryAfterMs(HttpResponse<?> res) {
    return res.headers().firstValue("retry-after").map(v -> {
      try {
        return Long.parseLong(v.trim()) * 1000;
      } catch (NumberFormatException e) {
        return 0L;
      }
    }).orElse(0L);
  }
}
