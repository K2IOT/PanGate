package com.pangate.account.domain;

import java.util.Locale;

/**
 * A login as typed (account spec §4.2): with {@code @} an email, starting with {@code +} a phone, else a username. The
 * key is normalised the way that kind is stored. Never throws: a malformed login is just one that matches no account,
 * so it fails like every other wrong login.
 */
public record Login(LoginKind kind, String key) {

  public static Login parse(String raw) {
    String s = raw == null ? "" : raw.trim();
    if (s.indexOf('@') >= 0) {
      return new Login(LoginKind.EMAIL, s.toLowerCase(Locale.ROOT));
    }
    if (s.startsWith("+")) {
      return new Login(LoginKind.PHONE, s);
    }
    return new Login(LoginKind.USERNAME, s.toLowerCase(Locale.ROOT));
  }
}
