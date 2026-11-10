package com.pangate.auth.application;

/** No active signing key could be loaded (spec §6): token endpoints answer 503. */
public final class NoSigningKeyException extends IllegalStateException {
  public NoSigningKeyException() {
    super("no active signing key");
  }
}
