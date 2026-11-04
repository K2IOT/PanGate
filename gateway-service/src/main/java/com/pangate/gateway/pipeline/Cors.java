package com.pangate.gateway.pipeline;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;

/**
 * CORS for the browser fallbacks {@code /v1/sse} and {@code /v1/upstream}. No cookies are involved (credentials are
 * the HELLO body or an Authorization ticket), so no Allow-Credentials. A preflight is answered for any Origin: the
 * real request is checked against the tenant's Origin allowlist (spec §4.6 g) once the tenant is known, and only an
 * allowed Origin is echoed on it.
 */
final class Cors {

  static final String ALLOW_HEADERS = "content-type, authorization";
  static final int MAX_AGE_SEC = 600;

  private Cors() {}

  static void allow(HttpHeaders h, String origin) {
    h.set(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN, origin).set(HttpHeaderNames.VARY, "Origin");
  }

  static void preflight(HttpHeaders h, String origin) {
    allow(h, origin);
    h.set(HttpHeaderNames.ACCESS_CONTROL_ALLOW_METHODS, "POST")
        .set(HttpHeaderNames.ACCESS_CONTROL_ALLOW_HEADERS, ALLOW_HEADERS)
        .setInt(HttpHeaderNames.ACCESS_CONTROL_MAX_AGE, MAX_AGE_SEC);
  }
}
