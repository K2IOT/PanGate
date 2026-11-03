package com.pangate.starter.secrets;

/** Thrown when a named secret is missing from its source. */
public final class SecretNotFoundException extends RuntimeException {

  private final String secretName;
  private final String location;

  public SecretNotFoundException(String secretName, String location, Throwable cause) {
    super("Secret '" + secretName + "' not found at " + location, cause);
    this.secretName = secretName;
    this.location = location;
  }

  public String secretName() {
    return secretName;
  }

  public String location() {
    return location;
  }
}
