package com.pangate.gateway.auth;

import com.pangate.gateway.config.GatewayProperties;
import java.net.URI;
import java.net.http.HttpClient;
import java.time.Duration;

/** Picks the JWKS source from configuration: exactly one of jwks-uri or jwks-file. */
public final class JwksSources {

  private JwksSources() {}

  public static JwksSource from(GatewayProperties.Auth auth, HttpClient client) {
    boolean hasUri = auth.jwksUri() != null && !auth.jwksUri().isBlank();
    boolean hasFile = auth.jwksFile() != null && !auth.jwksFile().toString().isBlank();
    if (hasUri == hasFile) {
      throw new JwksConfigException(hasUri
          ? "Both gateway.auth.jwks-uri and gateway.auth.jwks-file are set"
          : "Neither gateway.auth.jwks-uri nor gateway.auth.jwks-file is set");
    }
    return hasUri
        ? new HttpJwksSource(URI.create(auth.jwksUri()), client, Duration.ofSeconds(5))
        : new FileJwksSource(auth.jwksFile());
  }
}
