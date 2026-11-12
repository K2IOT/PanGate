package com.pangate.chat.core;

/** ScyllaDB did not answer in time or failed: the operation may be retried (chat spec §8). */
public final class StoreUnavailableException extends RuntimeException {
  public StoreUnavailableException(String message, Throwable cause) {
    super(message, cause);
  }
}
