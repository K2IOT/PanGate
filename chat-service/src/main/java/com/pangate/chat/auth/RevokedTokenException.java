package com.pangate.chat.auth;

/** A valid connect token whose jti, device or user was revoked on {@code gw.control} → 403. */
public final class RevokedTokenException extends RuntimeException {

  public RevokedTokenException(String reason) {
    super(reason, null, false, false);
  }
}
