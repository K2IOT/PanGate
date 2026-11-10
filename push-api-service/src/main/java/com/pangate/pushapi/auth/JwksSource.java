package com.pangate.pushapi.auth;

import com.nimbusds.jose.jwk.JWKSet;

/** Fetches auth-service's JWKS; null or an exception means it is unavailable right now. */
@FunctionalInterface
public interface JwksSource {
  JWKSet fetch() throws Exception;
}
