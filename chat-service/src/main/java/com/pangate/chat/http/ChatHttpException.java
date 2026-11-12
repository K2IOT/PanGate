package com.pangate.chat.http;

/** A request the API refuses with {@code status} and problem {@code code}. */
public final class ChatHttpException extends RuntimeException {

  private final int status;
  private final String code;

  public ChatHttpException(int status, String code, String detail) {
    super(detail, null, false, false);
    this.status = status;
    this.code = code;
  }

  public int status() {
    return status;
  }

  public String code() {
    return code;
  }
}
