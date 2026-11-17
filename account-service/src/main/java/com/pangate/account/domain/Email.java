package com.pangate.account.domain;

import java.util.Locale;

/** Email (account spec §4.1): trimmed, lower case, one {@code @}, a dot in the domain, at most 254 characters. */
public record Email(String value) {

  public static final int MAX = 254;

  public static Email of(String raw) {
    String s = raw == null ? "" : raw.trim().toLowerCase(Locale.ROOT);
    int at = s.indexOf('@');
    boolean ok = s.length() <= MAX && at > 0 && at == s.lastIndexOf('@') && s.chars().noneMatch(Character::isWhitespace);
    if (ok) {
      String domain = s.substring(at + 1);
      ok = domain.indexOf('.') > 0 && !domain.endsWith(".") && !domain.contains("..");
    }
    if (!ok) {
      throw new DomainException("invalid_email", "not an email address");
    }
    return new Email(s);
  }
}
