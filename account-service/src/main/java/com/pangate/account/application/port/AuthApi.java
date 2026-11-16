package com.pangate.account.application.port;

import java.time.Instant;

/** auth-service as account-service uses it (account spec §2): mint connect tokens, revoke them. */
public interface AuthApi {

  record ConnectToken(String token, Instant expiresAt) {}

  /** {@code POST /v1/tokens} for {@code chat:<userId>} on {@code deviceId}, 600 s, no topics. */
  ConnectToken mint(String userId, String deviceId);

  /** {@code POST /v1/revocations} with this JSON body. */
  void revoke(byte[] revocationJson);

  /** No answer, 5xx or 429: try again later. */
  final class AuthUnavailableException extends RuntimeException {
    public AuthUnavailableException(String message) {
      super(message, null, false, false);
    }
  }

  /** Any other 4xx: retrying the same request will not help. */
  final class AuthRejectedException extends RuntimeException {
    private final int status;

    public AuthRejectedException(int status) {
      super("auth-service answered " + status, null, false, false);
      this.status = status;
    }

    public int status() {
      return status;
    }
  }
}
