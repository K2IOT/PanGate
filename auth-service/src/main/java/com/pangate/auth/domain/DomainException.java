package com.pangate.auth.domain;

/** A rule violation with a stable machine-readable {@code code} and the HTTP status the API maps it to. */
public class DomainException extends RuntimeException {

  private final String code;
  private final int httpStatus;

  public DomainException(String code, int httpStatus, String detail) {
    super(detail, null, false, false);
    this.code = code;
    this.httpStatus = httpStatus;
  }

  public DomainException(String code, int httpStatus) {
    this(code, httpStatus, code);
  }

  public static DomainException badRequest(String code, String detail) {
    return new DomainException(code, 400, detail);
  }

  public String code() {
    return code;
  }

  public int httpStatus() {
    return httpStatus;
  }
}
