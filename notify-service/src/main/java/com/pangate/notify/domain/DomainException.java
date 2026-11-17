package com.pangate.notify.domain;

/** A refused request with a stable problem {@code code} (offline notifications spec §5.1). */
public class DomainException extends RuntimeException {

  private final String code;

  public DomainException(String code, String message) {
    super(message, null, false, false);
    this.code = code;
  }

  public String code() {
    return code;
  }
}
