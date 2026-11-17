package com.pangate.devtools.cmd;

import com.pangate.devtools.client.ClientTls;
import com.pangate.devtools.client.CloseInfo;
import com.pangate.devtools.client.GatewayClient;
import com.pangate.devtools.client.Hellos;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * Account steps of {@code smoke --account} (account spec §0, §10): register, log in on a second device, log one
 * device out (only its connection closes 4003), lock (the other closes 4003, log in refused), unlock (log in and
 * connect at once).
 */
final class AccountSmoke {

  static final List<String> STEPS = List.of("ACCOUNT register", "ACCOUNT login", "ACCOUNT device logout",
      "ACCOUNT lock", "ACCOUNT unlock");
  static final Duration T = Duration.ofSeconds(10);

  private final AccountApi api;
  private final URI ws;
  private final ClientTls tls;
  private final List<GatewayClient> open = new ArrayList<>();

  AccountSmoke(AccountApi api, URI ws, ClientTls tls) {
    this.api = api;
    this.ws = ws;
    this.tls = tls;
  }

  GatewayClient connect(AccountApi.Account a) {
    GatewayClient c = GatewayClient.connect(ws, tls, T);
    open.add(c);
    c.hello(Hellos.jwt(api.connectToken(a.refreshToken()), a.deviceId()), T);
    return c;
  }

  static void closed4003(GatewayClient c, String what) {
    long t0 = System.nanoTime();
    CloseInfo closed = c.awaitClose(T);
    if (closed.code() != 4003) {
      throw new IllegalStateException(what + ": expected close 4003, got " + closed);
    }
    System.out.printf("%s -> close 4003 in %.1f ms%n", what, (System.nanoTime() - t0) / 1e6);
  }

  void run(Consumer<String> step) {
    try {
      step.accept(STEPS.get(0));
      AccountApi.Account a1 = api.register(ChatSmoke.user("acct"));
      step.accept(STEPS.get(1));
      AccountApi.Account a2 = api.login(a1.username());
      GatewayClient c1 = connect(a1);
      GatewayClient c2 = connect(a2);

      step.accept(STEPS.get(2));
      api.logout(api.connectToken(a2.refreshToken()), a1.deviceId());
      closed4003(c1, "device logout");
      c2.ping(T);

      step.accept(STEPS.get(3));
      api.lock(a1.userId());
      closed4003(c2, "lock");
      int status = api.loginResponse(a1.username()).statusCode();
      if (status != 403) {
        throw new IllegalStateException("login of a locked account answered " + status);
      }

      step.accept(STEPS.get(4));
      api.unlock(a1.userId());
      connect(api.login(a1.username())).ping(T);
      System.out.println("ACCOUNT unlock -> log in and connect again");
    } finally {
      for (GatewayClient x : open) {
        try {
          x.close();
        } catch (RuntimeException ignored) {
          // closed
        }
      }
    }
  }
}
