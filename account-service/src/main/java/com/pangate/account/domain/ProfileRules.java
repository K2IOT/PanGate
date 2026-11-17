package com.pangate.account.domain;

/** Display name 1..64 and bio up to 140 characters (code points), no control characters (account spec §4.1). */
public final class ProfileRules {

  public static final int DISPLAY_NAME_MAX = 64;
  public static final int BIO_MAX = 140;

  private ProfileRules() {}

  public static String displayName(String raw) {
    String s = raw == null ? "" : raw.trim();
    int n = s.codePointCount(0, s.length());
    if (n < 1 || n > DISPLAY_NAME_MAX || s.codePoints().anyMatch(Character::isISOControl)) {
      throw new DomainException("invalid_display_name", "a display name is 1.." + DISPLAY_NAME_MAX + " characters");
    }
    return s;
  }

  /** Null stays null (no bio); a newline is allowed, other control characters are not. */
  public static String bio(String raw) {
    if (raw == null) {
      return null;
    }
    String s = raw.trim();
    if (s.codePointCount(0, s.length()) > BIO_MAX
        || s.codePoints().anyMatch(c -> Character.isISOControl(c) && c != '\n')) {
      throw new DomainException("invalid_bio", "a bio is at most " + BIO_MAX + " characters");
    }
    return s;
  }
}
