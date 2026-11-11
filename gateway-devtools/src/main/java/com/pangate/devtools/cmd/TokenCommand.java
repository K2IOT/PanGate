package com.pangate.devtools.cmd;

import com.pangate.devtools.jwt.JwtMinter;
import com.pangate.devtools.jwt.TokenRequest;
import com.pangate.devtools.keys.DevKeys;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "token", description = "Print a connect JWT: signed offline (--key) or minted by auth-service "
    + "(--auth-url).")
public final class TokenCommand implements Callable<Integer> {

  static final class Source {
    @Option(names = "--key", required = true, description = "ES256 key file (SEC1 PEM); created if missing.")
    Path key;

    @ArgGroup(exclusive = false)
    AuthOptions auth;
  }

  @ArgGroup(exclusive = true, multiplicity = "1")
  Source source;

  @Option(names = "--tenant", description = "Tenant claim (offline signing only; Auth takes it from the client).")
  String tenant;

  @Option(names = "--principal", required = true, description = "tenantId:userId")
  String principal;

  @Option(names = "--device", required = true)
  String device;

  @Option(names = "--topic", description = "Topic pattern claim; repeatable.")
  List<String> topics = new ArrayList<>();

  @Option(names = "--ttl", defaultValue = "PT10M", description = "ISO-8601 duration (default ${DEFAULT-VALUE}).")
  Duration ttl;

  @Override
  public Integer call() {
    if (source.auth != null) {
      System.out.println(source.auth.client().connectToken(principal, device, topics));
      return 0;
    }
    String t = tenant != null ? tenant : principal.substring(0, Math.max(0, principal.indexOf(':')));
    System.out.println(new JwtMinter(DevKeys.loadOrCreateEs256(source.key), "pangate-devtools")
        .mint(new TokenRequest(t, principal, device, topics, ttl), Instant.now()));
    return 0;
  }
}
