package com.pangate.gateway.auth;

import com.pangate.starter.secrets.SecretSource;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Optional;

/** The cell's three live ticket keys (spec §4.7): sign with current, accept prev, current and next. */
public final class TicketKeyRing {

  private final TicketKey current;
  private final List<TicketKey> all;

  public TicketKeyRing(TicketKey prev, TicketKey current, TicketKey next) {
    this.current = current;
    this.all = List.of(prev, current, next);
  }

  /** Reads {@code ticket-keys/{prev,current,next}}, each one line {@code "<kid> <base64 32 bytes>"}. */
  public static TicketKeyRing load(SecretSource secrets) {
    return new TicketKeyRing(read(secrets, "prev"), read(secrets, "current"), read(secrets, "next"));
  }

  private static TicketKey read(SecretSource secrets, String role) {
    String name = "ticket-keys/" + role;
    String line = new String(secrets.get(name), StandardCharsets.US_ASCII).trim();
    String[] parts = line.split("\\s+");
    try {
      if (parts.length != 2) {
        throw new IllegalArgumentException("expected '<kid> <base64>'");
      }
      return new TicketKey(Integer.parseInt(parts[0]), Base64.getDecoder().decode(parts[1]));
    } catch (IllegalArgumentException e) {
      throw new IllegalStateException("Invalid ticket key " + secrets.describe(name) + ": " + e.getMessage(), e);
    }
  }

  public TicketKey current() {
    return current;
  }

  public Optional<TicketKey> byKid(int kid) {
    for (TicketKey k : all) {
      if (k.kid() == kid) {
        return Optional.of(k);
      }
    }
    return Optional.empty();
  }
}
