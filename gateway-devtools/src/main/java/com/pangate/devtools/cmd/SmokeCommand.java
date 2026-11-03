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

@Command(name = "smoke", description = "End-to-end check: token -> HELLO -> PING -> close -> resume with ticket.")
public final class SmokeCommand implements Callable<Integer> {

  private static final Duration T = Duration.ofSeconds(10);
  private static final String DEVICE = "smoke-1";

  @Option(names = "--url", required = true)
  URI url;

  @Option(names = "--issuer", required = true, description = "Issuer base URL, e.g. http://issuer:8090")
  URI issuer;

  @ArgGroup(exclusive = true, multiplicity = "1")
  ClientCommand.Trust trust;

  @Override
  public Integer call() {
    String step = "mint token";
    try {
      String token = mintToken();
      ClientTls tls = trust.insecure ? ClientTls.insecure() : ClientTls.trust(trust.ca);

      step = "connect + HELLO (jwt)";
      Connected first;
      try (GatewayClient c = GatewayClient.connect(url, tls, T)) {
        first = c.hello(Hellos.jwt(token, DEVICE), T);
        step = "PING";
        c.ping(T);
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
      System.out.println("SMOKE OK");
      return 0;
    } catch (Exception e) {
      System.out.println("SMOKE FAILED at step '" + step + "': " + e);
      return 1;
    }
  }

  private String mintToken() throws Exception {
    HttpResponse<String> res = HttpClient.newHttpClient().send(
        HttpRequest.newBuilder(issuer.resolve("/tokens")).timeout(T)
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(
                "{\"tenant\":\"t1\",\"principal\":\"t1:smoke\",\"device\":\"" + DEVICE + "\"}")).build(),
        HttpResponse.BodyHandlers.ofString());
    if (res.statusCode() != 200) {
      throw new IllegalStateException("issuer answered " + res.statusCode() + ": " + res.body());
    }
    return JSONObjectUtils.parse(res.body()).get("token").toString();
  }
}
