package com.pangate.chat.core;

/** Why a send was refused (chat spec §4.3): invalid, too_long, not_found, not_member or rate_limited. */
public record Rejection(String reason, String detail) {

  public static final String INVALID = "invalid";
  public static final String TOO_LONG = "too_long";
  public static final String NOT_FOUND = "not_found";
  public static final String NOT_MEMBER = "not_member";
  public static final String RATE_LIMITED = "rate_limited";
}
