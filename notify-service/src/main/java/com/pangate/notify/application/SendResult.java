package com.pangate.notify.application;

/** What APNs or FCM answered for one token (offline notifications spec §5.4). */
public sealed interface SendResult {

  record Ok() implements SendResult {}

  /** The token is no longer valid: removed if the device still holds it. */
  record TokenDead() implements SendResult {}

  /** Throttled, a server error or no answer: tried again after {@code retryAfterMs} (0 = the default pause). */
  record Transient(long retryAfterMs) implements SendResult {}

  /** Our key or certificate was refused: not retried, logged. */
  record AuthFailed() implements SendResult {}

  /** Any other refusal of this request: not retried. */
  record Rejected(String reason) implements SendResult {}
}
