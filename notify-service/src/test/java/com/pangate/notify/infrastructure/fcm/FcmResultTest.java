package com.pangate.notify.infrastructure.fcm;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.notify.application.SendResult;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;
import java.util.Optional;
import javax.net.ssl.SSLSession;
import org.junit.jupiter.api.Test;

/** CH4 deferred minors: FCM answers that were mapped to "auth failed" for good. */
class FcmResultTest {

  final FcmProvider fcm = new FcmProvider(HttpClient.newHttpClient(), "http://unused", "p",
      new FcmAccessToken(HttpClient.newHttpClient(), "x@y", null, "http://unused/token", () -> 0L), () -> 0L);

  static HttpResponse<String> res(int status, String body) {
    return new HttpResponse<>() {
      public int statusCode() {
        return status;
      }

      public HttpRequest request() {
        return null;
      }

      public Optional<HttpResponse<String>> previousResponse() {
        return Optional.empty();
      }

      public HttpHeaders headers() {
        return HttpHeaders.of(Map.of(), (a, b) -> true);
      }

      public String body() {
        return body;
      }

      public Optional<SSLSession> sslSession() {
        return Optional.empty();
      }

      public URI uri() {
        return URI.create("http://unused");
      }

      public HttpClient.Version version() {
        return HttpClient.Version.HTTP_1_1;
      }
    };
  }

  @Test
  void senderIdMismatchMeansTheTokenIsNotOursAndIsRemoved() {
    assertThat(fcm.result(res(403, "{\"error\":{\"status\":\"PERMISSION_DENIED\",\"details\":[{\"errorCode\":"
        + "\"SENDER_ID_MISMATCH\"}]}}"))).isInstanceOf(SendResult.TokenDead.class);
  }

  @Test
  void unauthenticatedIsTriedAgainWithAFreshAccessToken() {
    assertThat(fcm.result(res(401, "{\"error\":{\"status\":\"UNAUTHENTICATED\"}}")))
        .isInstanceOf(SendResult.Transient.class);
  }

  @Test
  void otherPermissionDeniedIsStillAuthFailed() {
    assertThat(fcm.result(res(403, "{\"error\":{\"status\":\"PERMISSION_DENIED\"}}")))
        .isInstanceOf(SendResult.AuthFailed.class);
  }
}
