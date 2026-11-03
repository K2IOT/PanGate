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
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "token", description = "Sign a connect JWT offline and print it.")
public final class TokenCommand implements Callable<Integer> {

  @Option(names = "--key", required = true, description = "ES256 key file (SEC1 PEM); created if missing.")
  Path key;

  @Option(names = "--tenant", required = true)
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
    System.out.println(new JwtMinter(DevKeys.loadOrCreateEs256(key), "pangate-devtools")
        .mint(new TokenRequest(tenant, principal, device, topics, ttl), Instant.now()));
    return 0;
  }
}
