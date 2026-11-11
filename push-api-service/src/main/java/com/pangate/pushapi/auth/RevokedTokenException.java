package com.pangate.pushapi.auth;

/** A valid API token whose jti was revoked or whose client was disabled → 403. */
public final class RevokedTokenException extends RuntimeException {

  public RevokedTokenException(String reason) {
    super(reason, null, false, false);
  }
}
