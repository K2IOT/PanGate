package com.pangate.notify.infrastructure.fcm;

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
import java.util.function.LongSupplier;

/** FCM HTTP v1 (offline notifications spec §5.4, §5.5). */
public final class FcmProvider implements PushProvider {

  private final HttpClient http;
  private final String sendUrl;
  private final FcmAccessToken auth;
  private final LongSupplier wallMs;

  public FcmProvider(HttpClient http, String baseUrl, String projectId, FcmAccessToken auth, LongSupplier wallMs) {
    this.http = http;
    this.sendUrl = baseUrl + "/v1/projects/" + projectId + "/messages:send";
    this.auth = auth;
    this.wallMs = wallMs;
  }

  @Override
  public CompletableFuture<SendResult> send(PushToken t, NotifyRequest r) {
    String access;
    try {
      access = auth.token();
    } catch (Exception e) {
      return CompletableFuture.completedFuture(new SendResult.Transient(0));
    }
    HttpRequest req = HttpRequest.newBuilder(URI.create(sendUrl)).timeout(Duration.ofSeconds(10))
        .header("Authorization", "Bearer " + access).header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(FcmPayload.body(t.token(), r, wallMs.getAsLong()))).build();
    return http.sendAsync(req, HttpResponse.BodyHandlers.ofString()).thenApply(this::result)
        .exceptionally(e -> new SendResult.Transient(0));
  }

  SendResult result(HttpResponse<String> res) {
    int s = res.statusCode();
    String body = res.body() == null ? "" : res.body();
    if (s == 200) {
      return new SendResult.Ok();
    }
    if ((s == 404 && body.contains("UNREGISTERED"))
        || (s == 400 && body.contains("INVALID_ARGUMENT") && body.contains("registration token"))) {
      return new SendResult.TokenDead();
    }
    if (s == 403 && body.contains("SENDER_ID_MISMATCH")) {
      return new SendResult.TokenDead();   // CH4 deferred minor: the token belongs to another project, never ours
    }
    if (s == 401) {
      auth.forget();
      return new SendResult.Transient(0);   // CH4 deferred minor: an expired access token; the retry fetches a new one
    }
    if (s == 403) {
      auth.forget();
      return new SendResult.AuthFailed();
    }
    if (s == 429 || s >= 500) {
      return new SendResult.Transient(com.pangate.notify.infrastructure.apns.ApnsProvider.retryAfterMs(res));
    }
    return new SendResult.Rejected(s + " " + body);
  }
}
