package com.pangate.devtools.cmd;

import com.pangate.devtools.auth.AuthClient;
import java.net.URI;
import java.nio.file.Path;
import picocli.CommandLine.Option;

/** Tokens from auth-service: client credentials with a secret from the seed file. */
public final class AuthOptions {

  @Option(names = "--auth-url", required = true, description = "auth-service base URL, e.g. http://auth:8090")
  URI authUrl;

  @Option(names = "--client-file", required = true,
      description = "Seed secrets written by auth-service, e.g. .dev/auth/clients.json")
  Path clientFile;

  @Option(names = "--client", defaultValue = "t1-backend", description = "API client id (default ${DEFAULT-VALUE}).")
  String client;

  AuthClient client() {
    return new AuthClient(authUrl, clientFile, client);
  }
}
