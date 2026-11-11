package com.pangate.auth.domain;

import java.util.EnumSet;
import java.util.Set;
import java.util.stream.Collectors;

/** API scopes (spec §4.1); {@link #ADMIN} only for clients of the operator tenant. */
public enum Scope {
  TOKENS_MINT("tokens:mint"),
  SESSIONS_REVOKE("sessions:revoke"),
  PUSH_SEND("push:send"),
  PUSH_BROADCAST("push:broadcast"),
  PRESENCE_READ("presence:read"),
  DEVICES_WRITE("devices:write"),
  ADMIN("admin");

  private final String value;

  Scope(String value) {
    this.value = value;
  }

  public String value() {
    return value;
  }

  public static Scope of(String value) {
    for (Scope s : values()) {
      if (s.value.equals(value)) {
        return s;
      }
    }
    throw DomainException.badRequest("invalid_scope", "unknown scope: " + value);
  }

  /** Parses a space-separated scope string; null or blank is the empty set. */
  public static Set<Scope> parse(String spaceSeparated) {
    Set<Scope> out = EnumSet.noneOf(Scope.class);
    if (spaceSeparated == null) {
      return out;
    }
    for (String part : spaceSeparated.trim().split(" +")) {
      if (!part.isEmpty()) {
        out.add(of(part));
      }
    }
    return out;
  }

  /** Space-separated, in declaration order. */
  public static String format(Set<Scope> scopes) {
    return EnumSet.copyOf(scopes.isEmpty() ? EnumSet.noneOf(Scope.class) : scopes).stream()
        .map(Scope::value).collect(Collectors.joining(" "));
  }
}
