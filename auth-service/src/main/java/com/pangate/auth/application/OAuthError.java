package com.pangate.auth.application;

/** An RFC 6749 §5.2 error of the token endpoint. */
public final class OAuthError extends RuntimeException {

  private final String error;
  private final int status;

  public OAuthError(String error, int status, String description) {
    super(description, null, false, false);
    this.error = error;
    this.status = status;
  }

  public static OAuthError invalidClient() {
    return new OAuthError("invalid_client", 401, "client authentication failed");
  }

  public String error() {
    return error;
  }

  public int status() {
    return status;
  }
}
