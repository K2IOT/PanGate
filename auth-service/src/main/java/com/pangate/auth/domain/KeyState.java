package com.pangate.auth.domain;

/** Signing key life cycle (spec §3): published before use, kept after use for the overlap. */
public enum KeyState {
  PENDING, ACTIVE, RETIRED;

  public String db() {
    return name().toLowerCase(java.util.Locale.ROOT);
  }

  public static KeyState fromDb(String s) {
    return valueOf(s.toUpperCase(java.util.Locale.ROOT));
  }
}
