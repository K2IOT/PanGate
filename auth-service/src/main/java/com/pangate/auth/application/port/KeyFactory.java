package com.pangate.auth.application.port;

import com.pangate.auth.domain.KeyState;
import com.pangate.auth.domain.SigningKey;
import java.time.Instant;

/** Generates a fresh signing key (ES256, kid = RFC 7638 thumbprint in the crypto adapter). */
@FunctionalInterface
public interface KeyFactory {
  SigningKey newKey(Instant activateAt, KeyState state);
}
