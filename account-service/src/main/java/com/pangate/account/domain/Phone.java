package com.pangate.account.domain;

import java.util.regex.Pattern;

/** Phone number in E.164 as the client sends it (account spec §4.1); the server never guesses a country code. */
public record Phone(String value) {

  private static final Pattern E164 = Pattern.compile("^\\+[1-9][0-9]{6,14}$");

  public static Phone of(String raw) {
    String s = raw == null ? "" : raw.trim();
    if (!E164.matcher(s).matches()) {
      throw new DomainException("invalid_phone", "a phone number is E.164, e.g. +84901234567");
    }
    return new Phone(s);
  }
}
