package com.pangate.account.domain;

import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Username (account spec §4.1): {@code ^[a-zA-Z][a-zA-Z0-9_]{4,31}$}, compared without case; the spelling the user
 * chose is kept for display. Reserved names (lower case) are refused.
 */
public record Username(String display, String key) {

  private static final Pattern RULE = Pattern.compile("^[a-zA-Z][a-zA-Z0-9_]{4,31}$");

  public static Username of(String raw, Set<String> reserved) {
    String s = raw == null ? "" : raw.trim();
    if (!RULE.matcher(s).matches()) {
      throw new DomainException("invalid_username", "a username is 5..32 letters, digits or _, starting with a letter");
    }
    String key = s.toLowerCase(Locale.ROOT);
    if (reserved.contains(key)) {
      throw new DomainException("invalid_username", "this username is reserved");
    }
    return new Username(s, key);
  }
}
