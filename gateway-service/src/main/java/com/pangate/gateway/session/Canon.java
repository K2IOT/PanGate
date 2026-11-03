package com.pangate.gateway.session;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Bounded canonicalization of low-cardinality identity strings (tenant, platform, SDK version) so idle sessions
 * share one instance. Unlike {@link String#intern()}, client-chosen values cannot grow it without bound.
 */
final class Canon {

  private static final int MAX_ENTRIES = 4096;
  private static final ConcurrentHashMap<String, String> VALUES = new ConcurrentHashMap<>();

  private Canon() {}

  static String of(String s) {
    if (s == null) {
      return null;
    }
    String existing = VALUES.get(s);
    if (existing != null) {
      return existing;
    }
    if (VALUES.size() >= MAX_ENTRIES) {
      return s;
    }
    existing = VALUES.putIfAbsent(s, s);
    return existing != null ? existing : s;
  }
}
