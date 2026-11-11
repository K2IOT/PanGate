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
import java.util.Map;
import java.util.concurrent.Callable;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "smoke",
    description = "End-to-end check: token -> HELLO -> PING -> UPSTREAM cmd -> push (via Kafka and the push-router "
        + "with --kafka, else via fake-router) -> ACK -> close -> resume with ticket; with --kafka also OFFLINE; "
        + "with --auth-url also revoke -> close 4003.")
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

  @Option(names = "--router", description = "Gateway gRPC address for the fake router, e.g. gateway:9090")
  String router;

  @Option(names = "--kafka", description = "Kafka bootstrap: push through cN.push.requests and the push-router")
  String kafka;

  @Option(names = "--redis", description = "Redis state URI for seq:{p} of the Kafka push, e.g. redis://redis:6379")
  String redis;

  @Option(names = "--cell", defaultValue = "c1")
  String cell;

  @Option(names = "--no-router", description = "push via fake-router even when --kafka is given")
  boolean noRouter;

  @Option(names = "--push-api", description = "Push API base URL: push through POST /v1/push (needs --auth-url)")
  URI pushApi;

  @picocli.CommandLine.Spec
  picocli.CommandLine.Model.CommandSpec spec;

  boolean viaKafka() {
    return kafka != null && !noRouter;
  }

  boolean viaRouter() {
    return pushApi != null || viaKafka();
  }

  /** POST /v1/push of a RELIABLE message; returns its msgId. */
  private String apiPush(String token, String target, boolean receipt) throws Exception {
    String body = "{\"target\":{\"type\":\"principal\",\"id\":\"" + target + "\"},\"receipt\":" + receipt
        + (receipt ? ",\"senderPrincipalId\":\"t1:smoke-svc\"" : "") + ",\"payload\":{\"type\":\"smoke\"}}";
    HttpResponse<String> res = HttpClient.newHttpClient().send(HttpRequest.newBuilder(pushApi.resolve("/v1/push"))
        .timeout(T).header("Authorization", "Bearer " + token).header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
    if (res.statusCode() != 202) {
      throw new IllegalStateException("POST /v1/push answered " + res.statusCode() + ": " + res.body());
    }
    return JSONObjectUtils.parse(res.body()).get("msgId").toString();
  }

  /** Polls GET /v1/messages/{id} until {@code state}, at most 10 s. */
  private Map<String, Object> awaitStatus(String token, String msgId, String state) throws Exception {
    long deadline = System.nanoTime() + T.toNanos();
    Map<String, Object> last = Map.of();
    HttpClient http = HttpClient.newHttpClient();
    while (System.nanoTime() < deadline) {
      HttpResponse<String> res = http.send(HttpRequest.newBuilder(pushApi.resolve("/v1/messages/" + msgId)).timeout(T)
          .header("Authorization", "Bearer " + token).GET().build(), HttpResponse.BodyHandlers.ofString());
      if (res.statusCode() == 200) {
        last = JSONObjectUtils.parse(res.body());
        if (state.equals(last.get("state"))) {
          return last;
        }
      }
      Thread.sleep(200);
    }
    throw new IllegalStateException("message " + msgId + " not " + state + " within " + T + " (last " + last + ")");
  }

  @ArgGroup(exclusive = true, multiplicity = "1")
  ClientCommand.Trust trust;

  @Override
  public Integer call() {
    if (pushApi != null && source.auth == null) {
      throw new picocli.CommandLine.ParameterException(spec.commandLine(),
          "--push-api needs an API token: use --auth-url and --client-file");
    }
    if (pushApi == null && !viaKafka() && router == null) {
      throw new picocli.CommandLine.ParameterException(spec.commandLine(),
          "Missing required option: '--router=<router>' (needed without --kafka, or with --no-router)");
    }
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
        if (pushApi != null) {
          step = "PUSH via API -> router -> ACK";
          String apiToken = source.auth.client().apiToken();
          long t0 = System.nanoTime();
          String pushId = apiPush(apiToken, principal, true);
          com.pangate.contracts.v1.Frame push = c.awaitPush(T);
          if (!pushId.equals(push.getMsgId())) {
            throw new IllegalStateException("expected PUSH " + pushId + ", got " + push.getMsgId());
          }
          c.ack(pushId);
          System.out.printf("PUSH via API: POST -> PUSH in %.1f ms%n", (System.nanoTime() - t0) / 1e6);

          step = "STATUS delivered";
          Map<String, Object> st = awaitStatus(apiToken, pushId, "delivered");
          System.out.println("STATUS delivered: " + st.get("lastStatus") + ", connections "
              + st.get("deliveredConnections"));

          step = "OFFLINE via API (no session -> offline NO_SESSION)";
          String nobody = "t1:nobody-" + java.util.UUID.randomUUID().toString().substring(0, 8);
          Map<String, Object> off = awaitStatus(apiToken, apiPush(apiToken, nobody, false), "offline");
          if (!"no_session".equals(off.get("offlineReason"))) {
            throw new IllegalStateException("expected offline no_session, got " + off);
          }
          System.out.println("OFFLINE: " + off.get("offlineReason") + " attempts=" + off.get("attempts"));
        } else if (viaKafka()) {
          step = "PUSH via Kafka -> router -> ACK";
          try (com.pangate.devtools.push.PushProducer p =
              com.pangate.devtools.push.PushProducer.connect(kafka, redis, cell)) {
            long t0 = System.nanoTime();
            String pushId = p.message(principal, null, com.pangate.contracts.v1.DeliveryClass.DELIVERY_CLASS_RELIABLE,
                "smoke-push".getBytes(), false).getMsgId();
            com.pangate.contracts.v1.Frame push = c.awaitPush(T);
            if (!pushId.equals(push.getMsgId())) {
              throw new IllegalStateException("expected PUSH " + pushId + ", got " + push.getMsgId());
            }
            c.ack(pushId);
            System.out.printf("PUSH via Kafka: produce -> PUSH in %.1f ms%n", (System.nanoTime() - t0) / 1e6);

            step = "OFFLINE (no session -> OfflineRecord NO_SESSION)";
            try (com.pangate.devtools.push.OfflineAwaiter offline =
                new com.pangate.devtools.push.OfflineAwaiter(kafka, cell, false)) {
              String nobody = "t1:nobody-" + java.util.UUID.randomUUID().toString().substring(0, 8);
              String id = p.message(nobody, null, com.pangate.contracts.v1.DeliveryClass.DELIVERY_CLASS_RELIABLE,
                  "smoke-offline".getBytes(), false).getMsgId();
              com.pangate.contracts.v1.OfflineRecord o = offline.await(id, T);
              if (o.getReason() != com.pangate.contracts.v1.OfflineRecord.Reason.REASON_NO_SESSION) {
                throw new IllegalStateException("expected REASON_NO_SESSION, got " + o.getReason());
              }
              System.out.println("OFFLINE: " + o.getReason() + " attempts=" + o.getAttempts());
            }
          }
        } else {
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
        System.out.println("SMOKE OK (connected, pong, upstream-ack, push-ack, " + (viaRouter() ? "offline, " : "")
            + "resume, revoke)");
        return 0;
      }
      System.out.println("SMOKE OK (connected, pong, upstream-ack, push-ack, " + (viaRouter() ? "offline, " : "")
          + "resume)");
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
