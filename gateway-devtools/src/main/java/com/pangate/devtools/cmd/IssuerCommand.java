package com.pangate.devtools.cmd;

import com.pangate.devtools.issuer.IssuerServer;
import com.pangate.devtools.keys.DevKeys;
import java.nio.file.Path;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "issuer", description = "Serve a dev JWKS and mint connect tokens (POST /tokens).")
public final class IssuerCommand implements Callable<Integer> {

  @Option(names = "--port", defaultValue = "8090", description = "HTTP port (default ${DEFAULT-VALUE}).")
  int port;

  @Option(names = "--key", required = true, description = "ES256 key file (SEC1 PEM); created if missing.")
  Path key;

  @Override
  public Integer call() throws Exception {
    IssuerServer server = new IssuerServer(DevKeys.loadOrCreateEs256(key), port);
    server.start();
    System.out.println("issuer listening on :" + server.port() + " (key " + key + ")");
    Runtime.getRuntime().addShutdownHook(new Thread(server::stop));
    new CountDownLatch(1).await();
    return 0;
  }
}
