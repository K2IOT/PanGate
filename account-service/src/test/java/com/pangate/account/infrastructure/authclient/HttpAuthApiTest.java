package com.pangate.account.infrastructure.authclient;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.pangate.account.application.port.AuthApi;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class HttpAuthApiTest {

  HttpServer server;
  final AtomicInteger tokenStatus = new AtomicInteger(200);
  final AtomicInteger revokeStatus = new AtomicInteger(202);
  volatile long tokenDelayMs;

  String start() throws Exception {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.setExecutor(java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor());
    server.createContext("/oauth2/token", ex -> {
      try {
        Thread.sleep(tokenDelayMs);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      byte[] b = "{\"access_token\":\"api\",\"expires_in\":900}".getBytes(StandardCharsets.UTF_8);
      ex.sendResponseHeaders(tokenStatus.get(), tokenStatus.get() == 200 ? b.length : -1);
      if (tokenStatus.get() == 200) {
        ex.getResponseBody().write(b);
      }
      ex.close();
    });
    server.createContext("/v1/revocations", ex -> {
      ex.sendResponseHeaders(revokeStatus.get(), -1);
      ex.close();
    });
    server.start();
    return "http://127.0.0.1:" + server.getAddress().getPort();
  }

  @AfterEach
  void stop() {
    if (server != null) {
      server.stop(0);
    }
  }

  /** Final review I1: auth refusing account-service's own credentials is not a reason to drop revocations. */
  @Test
  void ownCredentialProblemsAreUnavailableNotRejected() throws Exception {
    HttpAuthApi api = new HttpAuthApi(start(), "chat-backend", "s", Duration.ofSeconds(2));
    tokenStatus.set(401);
    assertThatThrownBy(() -> api.revoke("{}".getBytes())).isInstanceOf(AuthApi.AuthUnavailableException.class);
    tokenStatus.set(200);
    revokeStatus.set(403);
    assertThatThrownBy(() -> api.revoke("{}".getBytes())).isInstanceOf(AuthApi.AuthUnavailableException.class);
    revokeStatus.set(400);
    assertThatThrownBy(() -> api.revoke("{}".getBytes())).isInstanceOf(AuthApi.AuthRejectedException.class);
  }

  /**
   * Final review I2: while the API token is being fetched from a slow auth-service, other virtual threads of the
   * service keep running (no carrier pinned by a monitor).
   */
  @Test
  void slowTokenFetchDoesNotStallOtherVirtualThreads() throws Exception {
    HttpAuthApi api = new HttpAuthApi(start(), "chat-backend", "s", Duration.ofSeconds(5));
    tokenDelayMs = 1_500;
    int n = Runtime.getRuntime().availableProcessors() * 4;
    for (int i = 0; i < n; i++) {
      Thread.ofVirtual().start(() -> {
        try {
          api.revoke("{}".getBytes());
        } catch (RuntimeException ignored) {
          // not the point
        }
      });
    }
    Thread.sleep(300);   // every caller is now inside or waiting for the fetch
    long t0 = System.nanoTime();
    Thread quick = Thread.ofVirtual().start(() -> { });
    quick.join();
    assertThat((System.nanoTime() - t0) / 1_000_000).as("ms for an unrelated virtual thread").isLessThan(500);
  }
}
