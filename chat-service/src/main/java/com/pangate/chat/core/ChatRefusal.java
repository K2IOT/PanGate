package com.pangate.chat.core;

/** A management or read request the core refuses; {@link #code()} is the problem code of chat spec §7. */
public final class ChatRefusal extends RuntimeException {

  public static final String INVALID = "invalid_request";
  public static final String FORBIDDEN = "forbidden";
  public static final String NOT_FOUND = "not_found";

  private final String code;

  public ChatRefusal(String code, String detail) {
    super(detail, null, false, false);
    this.code = code;
  }

  public String code() {
    return code;
  }

  static ChatRefusal invalid(String detail) {
    return new ChatRefusal(INVALID, detail);
  }

  static ChatRefusal forbidden(String detail) {
    return new ChatRefusal(FORBIDDEN, detail);
  }

  static ChatRefusal notFound() {
    return new ChatRefusal(NOT_FOUND, "no such conversation");
  }
}
