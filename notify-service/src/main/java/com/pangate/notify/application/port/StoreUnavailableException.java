package com.pangate.notify.application.port;

/** PostgreSQL did not answer: 503 with Retry-After: 5. */
public final class StoreUnavailableException extends RuntimeException {

  public StoreUnavailableException(Throwable cause) {
    super("database unavailable", cause);
  }
}
