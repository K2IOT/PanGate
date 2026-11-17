package com.pangate.account.application.port;

/** PostgreSQL did not answer (account spec §7): 503 with Retry-After: 5. */
public final class StoreUnavailableException extends RuntimeException {

  public StoreUnavailableException(Throwable cause) {
    super("database unavailable", cause);
  }
}
