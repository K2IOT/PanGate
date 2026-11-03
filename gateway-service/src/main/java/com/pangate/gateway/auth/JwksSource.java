package com.pangate.gateway.auth;

import com.nimbusds.jose.jwk.JWKSet;
import java.io.IOException;
import java.time.Duration;

/** Where the JWKS comes from (spec §4.9). Implementations may block: they run on the offload executor. */
@FunctionalInterface
public interface JwksSource {

  /**
   * @param etag value from the previous {@link Updated}, or null
   * @throws IOException on any transport or format error
   */
  FetchResult fetch(String etag) throws IOException;

  /** Fixed refresh interval for this source (e.g. file polling), or null to follow cache headers. */
  default Duration fixedInterval() {
    return null;
  }

  sealed interface FetchResult permits Updated, NotModified {}

  /** @param maxAge Cache-Control max-age, or null */
  record Updated(JWKSet set, String etag, Duration maxAge) implements FetchResult {}

  record NotModified(Duration maxAge) implements FetchResult {}
}
