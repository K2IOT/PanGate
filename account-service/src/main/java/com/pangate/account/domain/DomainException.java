package com.pangate.account.domain;

/** A refused request with a stable problem {@code code} (account spec §4.1, §4.2); the API maps code → status. */
public class DomainException extends RuntimeException {

  private final String code;

  public DomainException(String code, String detail) {
    super(detail, null, false, false);
    this.code = code;
  }

  public String code() {
    return code;
  }
}
