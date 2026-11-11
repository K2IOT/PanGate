package com.pangate.pushapi.core;

/** Redis state timed out or failed: the request is answered CODE_UNAVAILABLE (push-api spec §5). */
public final class StoreUnavailableException extends RuntimeException {

  public StoreUnavailableException(String what, Throwable cause) {
    super(what, cause);
  }
}
