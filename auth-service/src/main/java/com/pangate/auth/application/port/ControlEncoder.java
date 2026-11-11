package com.pangate.auth.application.port;

import com.pangate.auth.domain.Revocation;
import java.time.Instant;

/** Serialises a revocation as a gw.control {@code ControlEvent}. */
@FunctionalInterface
public interface ControlEncoder {
  byte[] revoke(Revocation r, Instant issuedAt);
}
