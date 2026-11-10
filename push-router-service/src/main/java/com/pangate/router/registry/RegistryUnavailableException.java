package com.pangate.router.registry;

/** The registry did not answer in time: a temporary failure, the record is retried (push-router spec §4). */
public final class RegistryUnavailableException extends RuntimeException {
  public RegistryUnavailableException(String principalHash, Throwable cause) {
    super("registry unavailable for principal " + principalHash, cause);
  }
}
