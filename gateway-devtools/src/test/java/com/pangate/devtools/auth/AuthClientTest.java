package com.pangate.devtools.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class AuthClientTest {

  record Req(String method, String path, String auth, String contentType, String body) {}

  private final List<Req> requests = new CopyOnWriteArrayList<>();
  private final AtomicInteger tokenCalls = new AtomicInteger();
  private final AtomicLong nowSec = new AtomicLong(1_000);
  private volatile int mintStatus = 200;
  private HttpServer server;
  private URI base;
  @TempDir Path dir;
  private Path clients;

  @BeforeEach
  void start() throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/", this::handle);
    server.start();
    base = URI.create("http://127.0.0.1:" + server.getAddress().getPort());
    clients = dir.resolve("clients.json");
    Files.writeString(clients, "{\"t1-backend\": \"s3cr-et_\", \"pangate-admin\": \"x\"}");
  }

  @AfterEach
  void stop() {
    server.stop(0);
  }

  private void handle(HttpExchange ex) throws IOException {
    String body = new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
    String path = ex.getRequestURI().getPath();
    requests.add(new Req(ex.getRequestMethod(), path, ex.getRequestHeaders().getFirst("Authorization"),
        ex.getRequestHeaders().getFirst("Content-Type"), body));
    String out;
    int status = 200;
    switch (path) {
      case "/oauth2/token" -> out = "{\"access_token\":\"api-" + tokenCalls.incrementAndGet()
          + "\",\"token_type\":\"Bearer\",\"expires_in\":900,\"scope\":\"tokens:mint\"}";
      case "/v1/tokens" -> {
        status = mintStatus;
        out = status == 200 ? "{\"token\":\"connect-jwt\",\"expiresAt\":\"2026-11-10T00:10:00Z\"}"
            : "{\"code\":\"missing_scope\"}";
      }
      case "/v1/revocations" -> {
        status = 202;
        out = "{\"id\":1,\"controlKey\":\"revoke:p:t1:smoke\"}";
      }
      default -> {
        status = 404;
        out = "{}";
      }
    }
    byte[] b = out.getBytes(StandardCharsets.UTF_8);
    ex.sendResponseHeaders(status, b.length);
    ex.getResponseBody().write(b);
    ex.close();
  }

  private AuthClient client() {
    return new AuthClient(base, clients, "t1-backend", nowSec::get);
  }

  @Test
  void clientCredentialsIsFormEncodedWithBasicAuth() {
    client().apiToken();
    Req r = requests.getFirst();
    assertThat(r.method()).isEqualTo("POST");
    assertThat(r.contentType()).startsWith("application/x-www-form-urlencoded");
    assertThat(r.body()).isEqualTo("grant_type=client_credentials");
    assertThat(new String(Base64.getDecoder().decode(r.auth().substring("Basic ".length())), StandardCharsets.UTF_8))
        .isEqualTo("t1-backend:s3cr-et_");
  }

  @Test
  void apiTokenIsReusedUntil60SecondsBeforeExpiry() {
    AuthClient c = client();
    assertThat(c.apiToken()).isEqualTo("api-1");
    nowSec.addAndGet(839);
    assertThat(c.apiToken()).isEqualTo("api-1");
    nowSec.addAndGet(1);  // 840 s = 900 - 60
    assertThat(c.apiToken()).isEqualTo("api-2");
    assertThat(tokenCalls).hasValue(2);
  }

  @Test
  void connectTokenPostsJsonWithTheBearerToken() {
    assertThat(client().connectToken("t1:smoke", "smoke-1", List.of("t1:news.*"))).isEqualTo("connect-jwt");
    Req mint = requests.get(1);
    assertThat(mint.path()).isEqualTo("/v1/tokens");
    assertThat(mint.auth()).isEqualTo("Bearer api-1");
    assertThat(mint.contentType()).startsWith("application/json");
    assertThat(mint.body()).contains("\"principal\":\"t1:smoke\"").contains("\"device\":\"smoke-1\"")
        .contains("\"topics\":[\"t1:news.*\"]");
  }

  @Test
  void revokePrincipalPostsARevocation() {
    client().revokePrincipal("t1:smoke");
    Req r = requests.get(1);
    assertThat(r.path()).isEqualTo("/v1/revocations");
    assertThat(r.body()).contains("\"kind\":\"principal\"").contains("\"principal\":\"t1:smoke\"");
  }

  @Test
  void non2xxRaisesWithStatusAndBody() {
    mintStatus = 403;
    assertThatThrownBy(() -> client().connectToken("t1:u", "d", List.of())).isInstanceOf(AuthClient.AuthException.class)
        .hasMessageContaining("403").hasMessageContaining("missing_scope");
  }

  @Test
  void unknownClientInTheFileFailsClearly() {
    assertThatThrownBy(() -> new AuthClient(base, clients, "t9-nobody", nowSec::get).apiToken())
        .hasMessageContaining("t9-nobody").hasMessageContaining(clients.toString());
  }
}
