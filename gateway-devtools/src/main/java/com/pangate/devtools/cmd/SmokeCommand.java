package com.pangate.devtools.cmd;

import com.nimbusds.jose.util.JSONObjectUtils;
import com.pangate.contracts.v1.Connected;
import com.pangate.devtools.client.ClientTls;
import com.pangate.devtools.client.GatewayClient;
import com.pangate.devtools.client.Hellos;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.Callable;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "smoke",
    description = "End-to-end check: token -> HELLO -> PING -> UPSTREAM cmd -> push via fake-router -> ACK -> "
        + "DeliveryAck -> close -> resume with ticket; with --auth-url also revoke -> close 4003.")
public final class SmokeCommand implements Callable<Integer> {

  private static final Duration T = Duration.ofSeconds(10);
  private static final String DEVICE = "smoke-1";
  private String principal = "t1:smoke";

  /** With auth-service the run ends by revoking its principal for 24 h, so each run needs its own. */
  static String principalFor(boolean auth) {
    return auth ? "t1:smoke-" + java.util.UUID.randomUUID().toString().substring(0, 8) : "t1:smoke";
  }

  @Option(names = "--url", required = true)
  URI url;

  /** Exactly one: the devtools issuer (gateway ITs, profile local) or auth-service (devenv). */
  static final class TokenSource {
    @Option(names = "--issuer", required = true, description = "devtools issuer base URL, e.g. http://issuer:8090")
    URI issuer;

    @ArgGroup(exclusive = false)
    AuthOptions auth;
  }

  @ArgGroup(exclusive = true, multiplicity = "1")
  TokenSource source;

  @Option(names = "--router", required = true, description = "Gateway gRPC address for the fake router, e.g. gateway:9090")
  String router;

  @ArgGroup(exclusive = true, multiplicity = "1")
  ClientCommand.Trust trust;

  @Override
  public Integer call() {
    String step = "mint token";
    principal = principalFor(source.auth != null);
    try {
      String token = mintToken();
      ClientTls tls = trust.insecure ? ClientTls.insecure() : ClientTls.trust(trust.ca);

      step = "connect + HELLO (jwt)";
      Connected first;
      try (GatewayClient c = GatewayClient.connect(url, tls, T)) {
        first = c.hello(Hellos.jwt(token, DEVICE), T);
        step = "PING";
        c.ping(T);
        step = "UPSTREAM cmd -> UPSTREAM_ACK";
        String msgId = java.util.UUID.randomUUID().toString();
        com.pangate.contracts.v1.Frame answer = c.upstream("cmd", msgId, "smoke".getBytes(), T);
        if (answer.getType() != com.pangate.contracts.v1.FrameType.FRAME_TYPE_UPSTREAM_ACK) {
          throw new IllegalStateException("expected UPSTREAM_ACK, got " + answer.getType() + " "
              + com.pangate.contracts.v1.ErrorInfo.parseFrom(answer.getPayload()));
        }
        step = "push via fake-router -> ACK -> DeliveryAck";
        try (com.pangate.devtools.router.FakeRouter r = com.pangate.devtools.router.FakeRouter.connect(router)) {
          String pushId = java.util.UUID.randomUUID().toString();
          long t0 = System.nanoTime();
          long deliveryId = r.unicast(first.getConnId(), com.pangate.contracts.v1.DeliveryClass.DELIVERY_CLASS_RELIABLE,
              pushId, "smoke-push".getBytes());
          com.pangate.contracts.v1.Frame push = c.awaitPush(T);
          if (!pushId.equals(push.getMsgId())) {
            throw new IllegalStateException("expected PUSH " + pushId + ", got " + push.getMsgId());
          }
          c.ack(pushId);
          com.pangate.contracts.v1.AckItem ack = r.awaitAck(deliveryId, T);
          if (ack.getStatus() != com.pangate.contracts.v1.AckItem.Status.STATUS_ACKED) {
            throw new IllegalStateException("expected ACKED, got " + ack.getStatus());
          }
          System.out.printf("push -> ack -> DeliveryAck in %.1f ms%n", (System.nanoTime() - t0) / 1e6);
        }
      }

      step = "reconnect + HELLO (resume ticket)";
      try (GatewayClient c = GatewayClient.connect(url, tls, T)) {
        Connected second = c.hello(Hellos.ticket(first.getResumeTicket(), DEVICE), T);
        step = "session expiry preserved";
        if (second.getSessionExpiresAtMs() != first.getSessionExpiresAtMs()) {
          throw new IllegalStateException("expires_at changed: " + first.getSessionExpiresAtMs()
              + " -> " + second.getSessionExpiresAtMs());
        }
      }
      if (source.auth != null) {
        step = "REVOKE principal -> close 4003";
        try (GatewayClient c = GatewayClient.connect(url, tls, T)) {
          c.hello(Hellos.ticket(first.getResumeTicket(), DEVICE), T);
          long t0 = System.nanoTime();
          source.auth.client().revokePrincipal(principal);
          com.pangate.devtools.client.CloseInfo closed = c.awaitClose(T);
          if (closed.code() != 4003) {
            throw new IllegalStateException("expected close 4003, got " + closed);
          }
          System.out.printf("revoke -> close 4003 in %.1f ms%n", (System.nanoTime() - t0) / 1e6);
        }
        System.out.println("SMOKE OK (connected, pong, upstream-ack, push-ack, resume, revoke)");
        return 0;
      }
      System.out.println("SMOKE OK (connected, pong, upstream-ack, push-ack, resume)");
      return 0;
    } catch (Exception e) {
      System.out.println("SMOKE FAILED at step '" + step + "': " + e);
      return 1;
    }
  }

  private String mintToken() throws Exception {
    if (source.auth != null) {
      return source.auth.client().connectToken(principal, DEVICE, java.util.List.of());
    }
    HttpResponse<String> res = HttpClient.newHttpClient().send(
        HttpRequest.newBuilder(source.issuer.resolve("/tokens")).timeout(T)
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(
                "{\"tenant\":\"t1\",\"principal\":\"" + principal + "\",\"device\":\"" + DEVICE + "\"}")).build(),
        HttpResponse.BodyHandlers.ofString());
    if (res.statusCode() != 200) {
      throw new IllegalStateException("issuer answered " + res.statusCode() + ": " + res.body());
    }
    return JSONObjectUtils.parse(res.body()).get("token").toString();
  }
}
