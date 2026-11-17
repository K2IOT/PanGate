package com.pangate.account.infrastructure.crypto;

/** Every hashing slot of this pod is in use (account spec §7): the API answers 503 with Retry-After: 1. */
public final class BusyException extends RuntimeException {

  public BusyException() {
    super("password hashing is busy", null, false, false);
  }
}
