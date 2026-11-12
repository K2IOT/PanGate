package com.pangate.chat.auth;

/** Missing, malformed, wrongly signed, expired or non-chat connect token → 401 {@code invalid_token}. */
public final class InvalidTokenException extends RuntimeException {

  public InvalidTokenException(String reason) {
    super(reason, null, false, false);
  }
}
