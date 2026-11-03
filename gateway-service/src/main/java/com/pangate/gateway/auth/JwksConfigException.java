package com.pangate.gateway.auth;

/** Invalid JWKS source configuration (none or both of jwks-uri / jwks-file). */
public final class JwksConfigException extends RuntimeException {

  public JwksConfigException(String message) {
    super(message);
  }
}
