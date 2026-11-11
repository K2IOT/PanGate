package com.pangate.pushapi.auth;

/** Missing, malformed, wrongly signed, expired or foreign API token → 401. */
public final class InvalidTokenException extends RuntimeException {

  public InvalidTokenException(String reason) {
    super(reason, null, false, false);
  }
}
