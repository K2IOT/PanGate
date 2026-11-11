package com.pangate.pushapi.core;

/** A request body that cannot be mapped at all (push-api spec §2.4); {@code code} is the problem+json code. */
public final class BadRequestException extends RuntimeException {

  private final String code;

  public BadRequestException(String code, String detail) {
    super(detail, null, false, false);
    this.code = code;
  }

  public String code() {
    return code;
  }
}
