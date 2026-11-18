package com.pangate.devtools.cmd;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.Callable;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "notify", description = "notify-service client.", subcommands = NotifyCommand.Register.class)
public final class NotifyCommand implements Callable<Integer> {

  @Override
  public Integer call() {
    System.err.println("usage: notify register --url ... --connect-token ... --platform apns|fcm --push-token ...");
    return 2;
  }

  /** {@code PUT /v1/devices/self/push} with the device's connect token (offline notifications spec §5.1). */
  @Command(name = "register", description = "Register this device's push token.")
  static final class Register implements Callable<Integer> {

    @Option(names = "--url", defaultValue = "http://localhost:8094", description = "notify-service public API")
    String url;

    @Option(names = "--connect-token", required = true)
    String connectToken;

    @Option(names = "--platform", required = true, description = "apns or fcm")
    String platform;

    @Option(names = "--push-token", required = true)
    String pushToken;

    @Option(names = "--environment", description = "APNs only: production (default) or sandbox")
    String environment;

    @Override
    public Integer call() throws Exception {
      int status = register(url, connectToken, platform, pushToken, environment);
      System.out.println("register -> HTTP " + status);
      return status == 204 ? 0 : 1;
    }
  }

  /** Returns the HTTP status. */
  public static int register(String url, String connectToken, String platform, String pushToken, String environment)
      throws Exception {
    String body = "{\"platform\":\"" + platform + "\",\"token\":\"" + pushToken + "\""
        + (environment == null ? "" : ",\"environment\":\"" + environment + "\"") + "}";
    HttpResponse<String> r = HttpClient.newHttpClient().send(HttpRequest.newBuilder(URI.create(url
            + "/v1/devices/self/push")).timeout(Duration.ofSeconds(5)).header("Content-Type", "application/json")
        .header("Authorization", "Bearer " + connectToken).PUT(HttpRequest.BodyPublishers.ofString(body)).build(),
        HttpResponse.BodyHandlers.ofString());
    return r.statusCode();
  }
}
